/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.http.HttpResponse;
import org.apache.http.impl.client.DefaultServiceUnavailableRetryStrategy;
import org.apache.http.protocol.HttpContext;

import java.time.Duration;

import static com.joyent.manta.client.multipart.EncryptedMultipartManager.CONTEXT_KEY_MPU_ENCRYPTED;

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

    @Override
    public boolean retryRequest(HttpResponse response, int executionCount, HttpContext context) {
        final Object isEncryptedMPUPart = context.getAttribute(CONTEXT_KEY_MPU_ENCRYPTED);

        if (isEncryptedMPUPart != null && (Boolean) isEncryptedMPUPart) {
            return false;
        }

        return super.retryRequest(response, executionCount, context);
    }

}
