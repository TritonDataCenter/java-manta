/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import org.apache.http.protocol.HttpContext;

/**
 * Interface indicating that an implementing class checks the request/response context for a flag indicating that
 * automatic retries should not occur.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 * @since 3.2.3
 */
public interface HttpContextRetryCancellation {

    /**
     * The reserved context key name.
     */
    String CONTEXT_ATTRIBUTE_MANTA_RETRY_DISABLE = "manta.retry.disable";

    /**
     * Check if the given context contains the reserved key and it is enabled.
     *
     * @param context the context to check
     * @return whether or not retries are disabled
     */
    default boolean neverRetry(final HttpContext context) {
        final Object disableRetry = context.getAttribute(CONTEXT_ATTRIBUTE_MANTA_RETRY_DISABLE);

        return disableRetry instanceof Boolean && (Boolean) disableRetry;
    }
}
