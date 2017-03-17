/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.fasterxml.uuid.EthernetAddress;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;
import org.slf4j.MDC;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.util.Random;
import java.util.UUID;

/**
 * Add the request id for an HTTP header to the SLF4J MDC logging implementation. This
 * is useful because it allows us to view the request id in all of the logs associated
 * with the request.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class RequestIdInterceptor implements HttpRequestInterceptor {
    /**
     * Time-based UUID generator for generating request ids.
     */
    private static final TimeBasedGenerator TIME_BASED_GENERATOR;

    static {
        Random nonBlockingRandomness;

        try {
            nonBlockingRandomness = SecureRandom.getInstance("NativePRNGNonBlocking", "SUN");
        } catch (NoSuchAlgorithmException | NoSuchProviderException e) {
            nonBlockingRandomness = new Random(System.nanoTime());
        }

        // Fake ethernet address based on a random value
        final EthernetAddress ethernetAddress = EthernetAddress.constructMulticastAddress(
                nonBlockingRandomness);
        TIME_BASED_GENERATOR = Generators.timeBasedGenerator(ethernetAddress);
    }

    /**
     * Constant identifying the request id as a MDC attribute.
     */
    public static final String MDC_REQUEST_ID_STRING = "mantaRequestId";

    @Override
    public void process(final HttpRequest request, final HttpContext context) throws HttpException, IOException {
        final UUID id = TIME_BASED_GENERATOR.generate();
        final String requestId = id.toString();
        final Header idHeader = new BasicHeader(MantaHttpHeaders.REQUEST_ID, requestId);
        request.addHeader(idHeader);

        MDC.put(MDC_REQUEST_ID_STRING, requestId);
    }
}
