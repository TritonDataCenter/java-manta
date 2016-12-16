/*
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.http;

import org.apache.commons.lang3.Validate;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.protocol.HttpContext;
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
     * HTTP context object used to share state between HTTP requests.
     */
    private final HttpContext httpContext;

    /**
     * Creates a new instance using the passed in factory class.
     * @param connectionFactory factory class that creates configured connections
     */
    public MantaApacheHttpClientContext(final MantaConnectionFactory connectionFactory) {
        Validate.notNull(connectionFactory,
                "Connection factory must not be null");

        this.httpClient = connectionFactory.createConnection();
        this.httpContext = buildHttpContext();
    }

    /**
     * Builds a configured HTTP context object that is pre-configured for
     * using HTTP Signature authentication.
     *
     * @return configured HTTP context object
     */
    protected HttpContext buildHttpContext() {
        return HttpClientContext.create();
    }

    @Override
    public CloseableHttpClient getHttpClient() {
        return httpClient;
    }

    @Override
    public HttpContext getHttpContext() {
        return httpContext;
    }

    @Override
    public void close() throws IOException {
        MDC.remove(RequestIdInterceptor.MDC_REQUEST_ID_STRING);

        httpClient.close();
    }
}
