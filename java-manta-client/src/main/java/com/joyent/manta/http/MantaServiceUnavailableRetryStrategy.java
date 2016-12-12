/*
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.http;

import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.http.impl.client.DefaultServiceUnavailableRetryStrategy;

import java.time.Duration;

/**
 * Implementation of {@link org.apache.http.client.ServiceUnavailableRetryStrategy}
 * customized for use with Manta.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class MantaServiceUnavailableRetryStrategy extends DefaultServiceUnavailableRetryStrategy {
    /**
     * Hardcoded retry interval of 1 second.
     */
    private static final int RETRY_INTERVAL = (int)Duration.ofSeconds(1).toMillis();

    /**
     * Creates a new instance of the retry strategy configured using a
     * {@link ConfigContext} object.
     *
     * @param context Manta SDK configuration object
     */
    public MantaServiceUnavailableRetryStrategy(final ConfigContext context) {
        super(ObjectUtils.firstNonNull(context.getRetries(), DefaultsConfigContext.DEFAULT_HTTP_RETRIES),
                RETRY_INTERVAL);
    }
}
