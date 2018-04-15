/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.joyent.manta.config.ConfigContext;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import javax.net.ssl.SSLException;

import static org.apache.commons.lang3.Validate.notNull;

/**
 * Implementation of {@link HttpRequestRetryHandler} customized for use with
 * Manta.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class MantaHttpRequestRetryHandler extends DefaultHttpRequestRetryHandler {

    /**
     * Logger instance.
     */
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * List of all exception types that can't be retried.
     */
    protected static final List<Class<? extends IOException>> NON_RETRIABLE = Arrays.asList(
            InterruptedIOException.class,
            UnknownHostException.class,
            ConnectException.class,
            SSLException.class);

    /**
     * Key for HttpContext setting indicating the request should NOT be retried under any circumstances.
     */
    public static final String CONTEXT_ATTRIBUTE_MANTA_RETRY_DISABLE = "manta.retry.disable";

    /**
     * Nullable meter for keeping track of the count and rate of retries.
     */
    private final Meter retries;

    /**
     * Deprecated constructor.
     *
     * @param config configuration indicating retry count
     */
    @Deprecated
    public MantaHttpRequestRetryHandler(final ConfigContext config) {
        this(notNull(config).getRetries(), null);
    }

    /**
     * Creates a new instance with the passed configuration.
     *  @param retryCount     how many times to retry; 0 means no retries
     *
     */
    public MantaHttpRequestRetryHandler(final int retryCount) {
        this(retryCount, null);
    }

    /**
     * Creates a new instance with the passed configuration.
     *
     * @param retryCount     how many times to retry; 0 means no retries
     * @param metricRegistry potentially-null registry for tracking client metrics
     */
    public MantaHttpRequestRetryHandler(final int retryCount, final MetricRegistry metricRegistry) {
        super(retryCount, true, NON_RETRIABLE);

        if (metricRegistry != null) {
            this.retries = metricRegistry.meter("retries");
        } else {
            this.retries = null;
        }
    }

    @Override
    public boolean retryRequest(final IOException exception,
                                final int executionCount,
                                final HttpContext context) {
        notNull(context, "HTTP context cannot be null");

        final Object disableRetry = context.getAttribute(CONTEXT_ATTRIBUTE_MANTA_RETRY_DISABLE);

        if (disableRetry instanceof Boolean && (Boolean) disableRetry) {
            return false;
        }

        final boolean toBeRetried = super.retryRequest(exception, executionCount, context);

        if (logger.isDebugEnabled()) {
            if (toBeRetried) {
                String msg = String.format("Request failed, %d/%d retry.",
                        executionCount, getRetryCount());
                logger.debug(msg, exception);
            } else {
                logger.debug("Request failed, unable to retry.", exception);
            }
        }

        if (toBeRetried && retries != null) {
            retries.mark();
        }

        return toBeRetried;
    }
}
