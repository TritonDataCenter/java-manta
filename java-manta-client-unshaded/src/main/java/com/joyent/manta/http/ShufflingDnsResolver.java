/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import org.apache.http.conn.DnsResolver;
import org.slf4j.MDC;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Implementation {@link DnsResolver} that shuffles the results of a DNS query
 * upon every query, so that we get better distribution of connections to
 * different hosts backed by the same name.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class ShufflingDnsResolver implements DnsResolver {
    @Override
    public InetAddress[] resolve(final String host) throws UnknownHostException {
        final InetAddress[] addresses = InetAddress.getAllByName(host);
        shuffle(addresses);

        MDC.put("mantaLoadBalancerAddress", addresses[0].getHostAddress());

        return addresses;
    }

    /**
     * Shuffles an array of addresses that were returned from a DNS query.
     * Shuffle algorithm inspired by <a href="http://stackoverflow.com/a/1520212/33611">this stackoverflow post.</a>
     * @param addresses addresses to shuffle
     */
    private static void shuffle(final InetAddress[] addresses) {
        // Only shuffle if we have 2 or more addresses
        if (addresses.length < 2) {
            return;
        }

        Random random = ThreadLocalRandom.current();

        for (int i = addresses.length - 1; i > 0; i--) {
            int index = random.nextInt(i + 1);
            // Simple swap
            InetAddress a = addresses[index];
            addresses[index] = addresses[i];
            addresses[i] = a;
        }
    }
}
