/*
 * Copyright 2018 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.resolver.dns;

import io.netty.channel.EventLoop;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.UnstableApi;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static io.netty.util.internal.ObjectUtil.checkNotNull;
import static io.netty.util.internal.ObjectUtil.checkPositiveOrZero;

/**
 * Default implementation of {@link AuthoritativeDnsServerCache}, backed by a {@link ConcurrentMap}.
 */
@UnstableApi
public class DefaultAuthoritativeDnsServerCache implements AuthoritativeDnsServerCache {

    // Two years are supported by all our EventLoop implementations and so safe to use as maximum.
    // See also: https://github.com/netty/netty/commit/b47fb817991b42ec8808c7d26538f3f2464e1fa6
    private static final int MAX_SUPPORTED_TTL_SECS = (int) TimeUnit.DAYS.toSeconds(365 * 2);

    private static final ScheduledFuture<?> CANCELLED = new ScheduledFuture<Object>() {

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            // We ignore unit and always return the minimum value to ensure the TTL of the cancelled marker is
            // the smallest.
            return Long.MIN_VALUE;
        }

        @Override
        public int compareTo(Delayed o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isCancelled() {
            return true;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public Object get() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object get(long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }
    };

    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<Entries, ScheduledFuture>
            FUTURE_UPDATER = AtomicReferenceFieldUpdater.newUpdater(
                    Entries.class, ScheduledFuture.class, "expirationFuture");

    private final ConcurrentMap<String, Entries> resolveCache = PlatformDependent.newConcurrentHashMap();
    private final int minTtl;
    private final int maxTtl;

    /**
     * Create a cache that respects the TTL returned by the DNS server.
     */
    public DefaultAuthoritativeDnsServerCache() {
        this(0, MAX_SUPPORTED_TTL_SECS);
    }

    /**
     * Create a cache.
     *
     * @param minTtl the minimum TTL
     * @param maxTtl the maximum TTL
     */
    public DefaultAuthoritativeDnsServerCache(int minTtl, int maxTtl) {
        this.minTtl = Math.min(MAX_SUPPORTED_TTL_SECS, checkPositiveOrZero(minTtl, "minTtl"));
        this.maxTtl = Math.min(MAX_SUPPORTED_TTL_SECS, checkPositiveOrZero(maxTtl, "maxTtl"));
        if (minTtl > maxTtl) {
            throw new IllegalArgumentException(
                    "minTtl: " + minTtl + ", maxTtl: " + maxTtl + " (expected: 0 <= minTtl <= maxTtl)");
        }
    }

    /**
     * Returns the minimum TTL of the cached DNS resource records (in seconds).
     *
     * @see #maxTtl()
     */
    public int minTtl() {
        return minTtl;
    }

    /**
     * Returns the maximum TTL of the cached DNS resource records (in seconds).
     *
     * @see #minTtl()
     */
    public int maxTtl() {
        return maxTtl;
    }

    @Override
    public List<InetSocketAddress> get(String hostname) {
        checkNotNull(hostname, "hostname");

        Entries entries = resolveCache.get(hostname);
        return entries == null ? Collections.<InetSocketAddress>emptyList() : entries.get();
    }

    @Override
    public void cache(String hostname, InetSocketAddress address, long originalTtl, EventLoop loop) {
        checkNotNull(hostname, "hostname");
        checkNotNull(address, "address");
        checkNotNull(loop, "loop");
        if (maxTtl == 0) {
            return;
        }
        if (PlatformDependent.javaVersion() >= 7 && address.getHostString() == null) {
            // We only cache addresses that have also a host string as we will need it later when trying to replace
            // unresolved entries in the cache.
            return;
        }

        Entries entries = resolveCache.get(hostname);
        if (entries == null) {
            entries = new Entries(hostname);
            Entries oldEntries = resolveCache.putIfAbsent(hostname, entries);
            if (oldEntries != null) {
                entries = oldEntries;
            }
        }

        entries.add(address, Math.max(minTtl, (int) Math.min(maxTtl, originalTtl)), loop);
    }

    @Override
    public void clear() {
        while (!resolveCache.isEmpty()) {
            for (Iterator<Map.Entry<String, Entries>> i = resolveCache.entrySet().iterator(); i.hasNext();) {
                Map.Entry<String, Entries> e = i.next();
                i.remove();

                e.getValue().clearAndCancel();
            }
        }
    }

    @Override
    public boolean clear(String hostname) {
        checkNotNull(hostname, "hostname");
        Entries entries = resolveCache.remove(hostname);
        return entries != null && entries.clearAndCancel();
    }

    @Override
    public String toString() {
        return "DefaultAuthoritativeDnsServerCache(minTtl=" + minTtl + ", maxTtl=" + maxTtl + ", cached nameservers=" +
                resolveCache.size() + ')';
    }

    // Directly extend AtomicReference for intrinsics and also to keep memory overhead low.
    private final class Entries extends AtomicReference<List<InetSocketAddress>> implements Runnable {

        private final String hostname;
        // Needs to be package-private to be able to access it via the AtomicReferenceFieldUpdater
        volatile ScheduledFuture<?> expirationFuture;

        Entries(String hostname) {
            super(Collections.<InetSocketAddress>emptyList());
            this.hostname = hostname;
        }

        void add(InetSocketAddress addr, int ttl, EventLoop loop) {
            for (;;) {
                List<InetSocketAddress> entryList = get();
                if (!entryList.isEmpty()) {
                    // Create a new List for COW semantics
                    List<InetSocketAddress> newEntries = new ArrayList<InetSocketAddress>(entryList.size() + 1);
                    InetSocketAddress replacedEntry = null;

                    String nameServerName = addr.getHostName();
                    int i = 0;
                    do {
                        InetSocketAddress address = entryList.get(i);
                        if (replacedEntry != null || !address.getHostName().equalsIgnoreCase(nameServerName)) {
                            newEntries.add(address);
                        } else {
                            // Replace the old entry.
                            replacedEntry = address;
                            newEntries.add(addr);
                        }
                    } while (++i < entryList.size());

                    if (replacedEntry == null) {
                        newEntries.add(addr);
                    }
                    if (compareAndSet(entryList, Collections.unmodifiableList(newEntries))) {
                        scheduleCacheExpirationIfNeeded(ttl, loop);
                        return;
                    }
                } else if (compareAndSet(entryList, Collections.singletonList(addr))) {
                    scheduleCacheExpirationIfNeeded(ttl, loop);
                    return;
                }
            }
        }

        private void scheduleCacheExpirationIfNeeded(int ttl, EventLoop loop) {
            for (;;) {
                ScheduledFuture<?> oldFuture = FUTURE_UPDATER.get(this);
                if (oldFuture == null || oldFuture.getDelay(TimeUnit.SECONDS) > ttl) {
                    ScheduledFuture<?> newFuture = loop.schedule(this, ttl, TimeUnit.SECONDS);
                    // It is possible that
                    // 1. task will fire in between this line, or
                    // 2. multiple timers may be set if there is concurrency
                    // (1) Shouldn't be a problem because we will fail the CAS and then the next loop will see CANCELLED
                    //     so the ttl will not be less, and we will bail out of the loop.
                    // (2) This is a trade-off to avoid concurrency resulting in contention on a synchronized block.
                    if (FUTURE_UPDATER.compareAndSet(this, oldFuture, newFuture)) {
                        if (oldFuture != null) {
                            oldFuture.cancel(true);
                        }
                        break;
                    } else {
                        // There was something else scheduled in the meantime... Cancel and try again.
                        newFuture.cancel(true);
                    }
                } else {
                    break;
                }
            }
        }

        boolean clearAndCancel() {
            List<InetSocketAddress> entries = getAndSet(Collections.<InetSocketAddress>emptyList());
            if (entries.isEmpty()) {
                return false;
            }

            ScheduledFuture<?> expirationFuture = FUTURE_UPDATER.getAndSet(this, CANCELLED);
            if (expirationFuture != null) {
                expirationFuture.cancel(false);
            }

            return true;
        }

        @Override
        public void run() {
            // We always remove all entries for a hostname once one entry expire. This is not the
            // most efficient to do but this way we can guarantee that if a DnsResolver
            // be configured to prefer one ip family over the other we will not return unexpected
            // results to the enduser if one of the A or AAAA records has different TTL settings.
            //
            // As a TTL is just a hint of the maximum time a cache is allowed to cache stuff it's
            // completely fine to remove the entry even if the TTL is not reached yet.
            //
            // See https://github.com/netty/netty/issues/7329
            resolveCache.remove(hostname, this);

            clearAndCancel();
        }
    }
}
