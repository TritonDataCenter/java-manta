/*
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.http;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.protocol.HttpContext;

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
     * HTTP context object used to share state between HTTP requests.
     * @return HTTP connection context object
     */
    HttpContext getHttpContext();
}
