/*
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.http;

import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;
import org.slf4j.MDC;

import java.io.IOException;
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
     * Constant identifying the request id as a MDC attribute.
     */
    public static final String MDC_REQUEST_ID_STRING = "mantaRequestId";

    @Override
    public void process(final HttpRequest request, final HttpContext context) throws HttpException, IOException {
        final String requestId = UUID.randomUUID().toString();
        final Header idHeader = new BasicHeader(MantaHttpHeaders.REQUEST_ID, requestId);
        request.addHeader(idHeader);

        MDC.put(MDC_REQUEST_ID_STRING, requestId);
    }
}
