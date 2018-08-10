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

import io.netty.util.internal.ObjectUtil;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Comparator;

/**
 * Special {@link Comparator} implementation to sort the nameservers to use when follow redirects.
 *
 * This implementation follows all the semantics listed in with the limitation that
 * {@link InetSocketAddress#equals(Object)} will not result in the same return value as
 * {@link #compare(InetSocketAddress, InetSocketAddress)}. This is completely fine as we only use it to sort
 * an {@link java.util.List}.
 */
final class NameServerComparator implements Comparator<InetSocketAddress> {

    private final Class<? extends InetAddress> preferredAddressType;

    NameServerComparator(Class<? extends InetAddress> preferredAddressType) {
        this.preferredAddressType = ObjectUtil.checkNotNull(preferredAddressType, "preferredAddressType");
    }

    @Override
    public int compare(InetSocketAddress addr1, InetSocketAddress addr2) {
        if (addr1 == addr2) {
            return 0;
        }
        if (!addr1.isUnresolved()) {
            if (!addr2.isUnresolved()) {
                if (addr1.getAddress().getClass() == addr2.getAddress().getClass()) {
                    // If both addresses use the same type we will just return 0 to preserve the original order
                    // when sorting the List.
                    return 0;
                }
            }
            if (preferredAddressType.isAssignableFrom(addr1.getAddress().getClass())) {
                if (!addr2.isUnresolved() && preferredAddressType.isAssignableFrom(addr2.getAddress().getClass())) {
                    // If both addresses are of the preferred type we return 0 to preserve the original order
                    // when sorting the List.
                    return 0;
                }
                return -1;
            } else {
                if (addr2.isUnresolved()) {
                    // Prefer the resolved address.
                    return -1;
                }
                return 1;
            }
        } else if (!addr2.isUnresolved()) {
            return 1;
        }
        return 0;
    }
}
