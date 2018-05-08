/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.joyent.manta.config.MantaClientMetricConfiguration;
import com.joyent.manta.exception.MantaIOException;
import com.joyent.manta.exception.MantaNoHttpResponseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.NoHttpResponseException;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestExecutor;
import org.slf4j.MDC;

import java.io.IOException;

/**
 * Extended implementation of {@link HttpRequestExecutor} with Manta specific
 * extensions for logging and exception handling.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.1.7
 */
public class MantaHttpRequestExecutor extends HttpRequestExecutor {

    /**
     * Creates new instance of HttpRequestExecutor. We're delegating any concerns
     * about waitForContinue to the parent's parameterless constructor.
     */
    public MantaHttpRequestExecutor() {
        super();
    }

    /**
     * Creates new instance of HttpRequestExecutor.
     *
     * @param waitForContinue Maximum time in milliseconds to wait for a 100-continue response
     */
    public MantaHttpRequestExecutor(final int waitForContinue) {
        super(waitForContinue);
    }

    /**
     * Adds a context value for the Manta load balancer associated with the
     * request to the MDC object with the key <code>mantaLoadBalancerAddress</code>
     * and proxies the parent class
     * {@link HttpRequestExecutor#doSendRequest(HttpRequest, HttpClientConnection, HttpContext)}
     * method.
     *
     * {@inheritDoc}
     */
    @Override
    protected HttpResponse doSendRequest(final HttpRequest request,
                                         final HttpClientConnection conn,
                                         final HttpContext context) throws IOException, HttpException {
        MDC.put("mantaLoadBalancerAddress", extractLoadBalancerAddress(conn));
        return super.doSendRequest(request, conn, context);
    }

    /**
     * Proxies the parent class
     * {@link HttpRequestExecutor#doReceiveResponse(HttpRequest, HttpClientConnection, HttpContext)}
     * method and catches {@link IOException} instances thrown. Those exceptions
     * are then wrapped in a {@link MantaIOException} or
     * {@link MantaNoHttpResponseException} instance in order to provide
     * detailed information for debugging.
     *
     * {@inheritDoc}
     */
    @Override
    protected HttpResponse doReceiveResponse(
            final HttpRequest request,
            final HttpClientConnection conn,
            final HttpContext context) throws HttpException, IOException {
        HttpResponse response = null;

        try {
            response = super.doReceiveResponse(request, conn, context);

        /* We catch all IOExceptions and wrap then in a MantaIOException because
         * this allows us to capture key information like the request id and
         * load balancer address directly in the exception message. */
        } catch (IOException e) {
            final MantaIOException mioe;

            /* If the source exception is NoHttpResponseException we create
             * a MantaNoHttpResponseException, so that we can act upon that
             * exception type directly within Manta. */
            if (e instanceof NoHttpResponseException) {
                mioe = new MantaNoHttpResponseException(e);
            } else {
                mioe = new MantaIOException(e);
            }

            HttpHelper.annotateContextedException(mioe, request, response);

            if (request.getFirstHeader(MantaHttpHeaders.REQUEST_ID) != null) {
                mioe.setContextValue("requestId",
                        request.getFirstHeader(MantaHttpHeaders.REQUEST_ID).getValue());
            }

            mioe.setContextValue("loadBalancerAddress", extractLoadBalancerAddress(conn));

            throw mioe;
        }

        if (!response.containsHeader("x-load-balancer")) {
            response.setHeader("x-load-balancer", extractLoadBalancerAddress(conn));
        }

        return response;
    }

    /**
     * Extracts the remote load balancer IP address from the toString() method
     * of a {@link HttpClientConnection}.
     *
     * @param conn connection to extract IP information from
     * @return IP address string or null if connection is null
     */
    private static String extractLoadBalancerAddress(final HttpClientConnection conn) {
        if (conn == null) {
            return null;
        }

        return StringUtils.substringBetween(conn.toString(), "<->", ":");
    }

    static class Builder {
        private Integer waitForContinue;
        private MantaClientMetricConfiguration metricConfig;

        static Builder create() {
            return new Builder();
        }

        public Builder setWaitForContinue(final Integer waitForContinue) {
            this.waitForContinue = waitForContinue;
            return this;
        }

        public Builder setMetricConfiguration(final MantaClientMetricConfiguration metricConfiguration) {
            this.metricConfig = metricConfiguration;
            return this;
        }

        public MantaHttpRequestExecutor build() {
            if (metricConfig != null) {
                if (waitForContinue != null) {
                    return new InstrumentedMantaHttpRequestExecutor(metricConfig.getRegistry(), waitForContinue);
                }

                return new InstrumentedMantaHttpRequestExecutor(metricConfig.getRegistry());
            }

            if (waitForContinue != null) {
                return new MantaHttpRequestExecutor(waitForContinue);
            }

            return new MantaHttpRequestExecutor();
        }
    }
}
