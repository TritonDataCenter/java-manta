/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

/**
 * Interface indicating that an object understands the underlying retry behavior and a common key to use
 * when indicating that retry cancellation is possible through {@link HttpContextRetryCancellation}.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 * @since 3.2.3
 */
public interface RetryConfigAware {

    /**
     * Whether or not automatic retries are enabled.
     *
     * @return if retries are enabled
     */
    boolean isRetryEnabled();

    /**
     * Whether or not retries can be cancelled using {@link HttpContextRetryCancellation}.
     *
     * @return if retry cancellation is supported
     */
    boolean isRetryCancellable();
}
