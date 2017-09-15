/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import org.apache.commons.lang3.Validate;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.MDC;

import java.io.IOException;

/**
 * {@link AutoCloseable} class that encapsulates the functionality of multiple
 * closeable resources that are used when accessing Manta's API. This class can
 * be passed to any method that directly accesses the Manta API in order to more
 * efficiently use network resources.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class MantaApacheHttpClientContext implements MantaConnectionContext {
    /**
     * HTTP client object used for accessing Manta.
     */
    private final CloseableHttpClient httpClient;

    /**
     * Creates a new instance using the passed in factory class.
     * @param connectionFactory factory class that creates configured connections
     */
    public MantaApacheHttpClientContext(final MantaConnectionFactory connectionFactory) {
        Validate.notNull(connectionFactory,
                "Connection factory must not be null");

        this.httpClient = connectionFactory.createConnection();
    }

    @Override
    public CloseableHttpClient getHttpClient() {
        return httpClient;
    }

    @Override
    public void close() throws IOException {
        MDC.remove(RequestIdInterceptor.MDC_REQUEST_ID_STRING);

        httpClient.close();
    }
}
