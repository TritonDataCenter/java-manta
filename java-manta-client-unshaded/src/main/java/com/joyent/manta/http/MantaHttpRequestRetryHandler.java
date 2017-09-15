/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.joyent.manta.config.ConfigContext;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

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
     * Creates a new instance with the passed configuration.
     *
     * @param config configuration for retries
     */
    public MantaHttpRequestRetryHandler(final ConfigContext config) {
        super(config.getRetries(), true, NON_RETRIABLE);
    }

    @Override
    public boolean retryRequest(final IOException exception,
                                final int executionCount,
                                final HttpContext context) {
        final Object disableRetry = context.getAttribute(CONTEXT_ATTRIBUTE_MANTA_RETRY_DISABLE);

        if (disableRetry instanceof Boolean && (Boolean) disableRetry) {
            return false;
        }

        if (logger.isDebugEnabled() && executionCount <= getRetryCount()) {
            String msg = String.format("Request failed, %d/%d retry.",
                    executionCount, getRetryCount());
            logger.debug(msg, exception);
        }

        return super.retryRequest(exception, executionCount, context);
    }
}
