/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import org.apache.http.impl.client.CloseableHttpClient;

/**
 * Interface describing the contract for a context class that stores state between
 * requests to the Manta API.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public interface MantaConnectionContext extends AutoCloseable {
    /**
     * HTTP client object used for accessing Manta.
     * @return connection object to Manta
     */
    CloseableHttpClient getHttpClient();

    /**
     * HTTP Request creation object.
     * @return request creation object for use with the above connection
     */
    MantaHttpRequestFactory getRequestFactory();
}
