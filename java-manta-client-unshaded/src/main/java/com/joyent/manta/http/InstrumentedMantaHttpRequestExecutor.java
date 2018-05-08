/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.httpclient.HttpClientMetricNameStrategies;
import com.codahale.metrics.httpclient.HttpClientMetricNameStrategy;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Reimplementation of {@link com.codahale.metrics.httpclient.InstrumentedHttpRequestExecutor} which extends
 * {@link MantaHttpRequestExecutor}. We need to duplicate the instrumented executor ourselves since we depend on
 * certain behavior in {@link MantaHttpRequestExecutor}.
 */
public class InstrumentedMantaHttpRequestExecutor extends MantaHttpRequestExecutor {

    /**
     * Registry used to track metrics. Never null.
     */
    private final MetricRegistry registry;

    /**
     * Creates new instance of HttpRequestExecutor. We're delegating any concerns
     * about waitForContinue to the parent's parameterless constructor.
     *
     * @param registry the metric registry
     */
    public InstrumentedMantaHttpRequestExecutor(final MetricRegistry registry) {
        super();
        this.registry = registry;
    }

    /**
     * Creates new instance of HttpRequestExecutor.
     *
     * @param registry the metric registry
     * @param name the unique client identifier
     * @param waitForContinue Maximum time in milliseconds to wait for a 100-continue response
     */
    public InstrumentedMantaHttpRequestExecutor(final MetricRegistry registry,
                                                final int waitForContinue) {
        super(waitForContinue);
        this.registry = registry;
    }

    @Override
    public HttpResponse execute(final HttpRequest request,
                                final HttpClientConnection conn,
                                final HttpContext context)
            throws HttpException, IOException
    {
        final Timer.Context timerContext = timer(request).time();
        try {
            return super.execute(request, conn, context);
        } catch (final HttpException | IOException e) {
            meter(e).mark();
            throw e;
        } finally {
            timerContext.stop();
        }
    }

    /**
     * Get a reference to (or create) a {@link Timer} based on the supplied request.
     * @param request HttpRequest to be tracked
     * @return a timer within the registry
     */
    private Timer timer(final HttpRequest request) {
        return registry.timer(request.getRequestLine().getMethod().toLowerCase() + "-requests");
    }

    /**
     * Get a reference to (or create) a {@link Meter} based on the supplied exception.
     * @param e exception type to be tracked
     * @return a meter within the registry
     */
    private Meter meter(final Exception e) {
        final Throwable rootEx = ObjectUtils.firstNonNull(ExceptionUtils.getRootCause(e), e);

        return registry.meter(name(rootEx.getClass().getSimpleName()));
    }
}
