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
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.StringUtil;
import io.netty.util.internal.UnstableApi;

import java.net.InetSocketAddress;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.netty.util.internal.ObjectUtil.checkNotNull;
import static io.netty.util.internal.ObjectUtil.checkPositiveOrZero;

/**
 * Default implementation of {@link AuthoritativeDnsServerCache}, backed by a {@link ConcurrentMap}.
 * */
@UnstableApi
public class DefaultAuthoritativeDnsServerCache implements AuthoritativeDnsServerCache {

    // Two years are supported by all our EventLoop implementations and so safe to use as maximum.
    // See also: https://github.com/netty/netty/commit/b47fb817991b42ec8808c7d26538f3f2464e1fa6
    private static final int MAX_SUPPORTED_TTL_SECS = (int) TimeUnit.DAYS.toSeconds(365 * 2);

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

        Entries entries = resolveCache.get(appendDot(hostname));
        if (entries == null) {
            return Collections.emptyList();
        }
        return entries.get();
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

        final Entry e = new Entry(address);

        String key = appendDot(hostname);
        Entries entries = resolveCache.get(key);
        if (entries == null) {
            entries = new Entries(e);
            Entries oldEntries = resolveCache.putIfAbsent(key, entries);
            if (oldEntries != null) {
                entries = oldEntries;
            }
        }

        final long ttl;
        Entry old = entries.add(e);
        if (old != null) {
            ScheduledFuture<?> future = old.expirationFuture;
            if (future != null) {
                ttl = Math.min(originalTtl, future.getDelay(TimeUnit.SECONDS));
                future.cancel(false);
            } else {
                ttl = originalTtl;
            }
        } else {
            ttl = originalTtl;
        }
        scheduleCacheExpiration(key, e, Math.max(minTtl,
                Math.min(MAX_SUPPORTED_TTL_SECS, (int) Math.min(maxTtl, ttl))), loop);
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
        Entries entries = resolveCache.remove(appendDot(hostname));
        return entries != null && entries.clearAndCancel();
    }

    private void scheduleCacheExpiration(final String hostname, final Entry e, int ttl, EventLoop loop) {
        e.scheduleExpiration(loop, new Runnable() {
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
                Entries entries = resolveCache.remove(hostname);
                if (entries != null) {
                    entries.clearAndCancel();
                }
            }
        }, ttl, TimeUnit.SECONDS);
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("DefaultAuthoritativeDnsServerCache(minTtl=")
                .append(minTtl).append(", maxTtl=")
                .append(maxTtl).append(", cached nameservers=")
                .append(resolveCache.size()).append(")")
                .toString();
    }

    private static final class Entry {

        volatile ScheduledFuture<?> expirationFuture;

        private final InetSocketAddress address;

        Entry(InetSocketAddress address) {
            this.address = checkNotNull(address, "address");
        }

        InetSocketAddress nameServer() {
            return address;
        }

        void scheduleExpiration(EventLoop loop, Runnable task, long delay, TimeUnit unit) {
            assert expirationFuture == null : "expiration task scheduled already";
            expirationFuture = loop.schedule(task, delay, unit);
        }

        void cancelExpiration() {
            ScheduledFuture<?> expirationFuture = this.expirationFuture;
            if (expirationFuture != null) {
                expirationFuture.cancel(false);
            }
        }

        @Override
        public String toString() {
            return address.toString();
        }
    }

    // This List implementation always has RandomAccess semantics as we either wrap an ArrayList, empty List or
    // one which has only one element.
    private static final class EntryList extends AbstractList<InetSocketAddress> implements RandomAccess {

        static final EntryList EMPTY = new EntryList(Collections.<Entry>emptyList());

        private final List<Entry> entries;

        EntryList(Entry entry) {
            this(Collections.singletonList(entry));
        }

        EntryList(List<Entry> entries) {
            this.entries = entries;
        }

        @Override
        public InetSocketAddress get(int i) {
            return entries.get(i).address;
        }

        @Override
        public int size() {
            return entries.size();
        }

        Entry getEntry(int i) {
            return entries.get(i);
        }

        boolean cancelExpiration() {
            final int numEntries = size();
            if (numEntries == 0) {
                return false;
            }
            for (int i = 0; i < numEntries; i++) {
                entries.get(i).cancelExpiration();
            }
            return true;
        }
    }

    // Directly extend AtomicReference for intrinsics and also to keep memory overhead low.
    private static final class Entries extends AtomicReference<EntryList> {

        Entries(Entry entry) {
            super(new EntryList(entry));
        }

        Entry add(Entry e) {
            for (;;) {
                EntryList entryList = get();
                if (!entryList.isEmpty()) {
                    // Create a new List for COW semantics
                    List<Entry> newEntries = new ArrayList<Entry>(entryList.size() + 1);
                    Entry replacedEntry = null;

                    String nameServerName = e.nameServer().getHostName();
                    for (int i = 0; i < entryList.size(); i++) {
                        Entry entry = entryList.getEntry(i);
                        if (!entry.nameServer().getHostName().equalsIgnoreCase(nameServerName)) {
                            newEntries.add(entry);
                        } else {
                            // Replace the old entry.
                            assert replacedEntry == null;
                            replacedEntry = entry;
                            newEntries.add(e);
                        }
                    }
                    if (replacedEntry == null) {
                        newEntries.add(e);
                    }
                    if (compareAndSet(entryList, new EntryList(newEntries))) {
                        return replacedEntry;
                    }
                } else if (compareAndSet(entryList, new EntryList(e))) {
                    return null;
                }
            }
        }

        boolean clearAndCancel() {
            EntryList entryList = getAndSet(EntryList.EMPTY);
            return entryList.cancelExpiration();
        }
    }

    private static String appendDot(String hostname) {
        return StringUtil.endsWith(hostname, '.') ? hostname : hostname + '.';
    }
}
