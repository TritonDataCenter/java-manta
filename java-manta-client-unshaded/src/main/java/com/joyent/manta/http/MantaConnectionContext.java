/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.joyent.manta.util.MetricsAware;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;

/**
 * Interface describing the contract for a context class that stores state between requests to the Manta API.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public interface MantaConnectionContext extends AutoCloseable, RetryConfigAware, MetricsAware {

    /**
     * HTTP client object used for accessing Manta.
     *
     * @return connection object to Manta
     */
    CloseableHttpClient getHttpClient();

    /**
     * {@inheritDoc}
     *
     * <p>Note: This changes the signature of {@link AutoCloseable#close()} to
     * only throw {@link IOException}.</p>
     *
     * @throws IOException thrown if there was a problem closing resources
     */
    @Override
    void close() throws IOException;
}
