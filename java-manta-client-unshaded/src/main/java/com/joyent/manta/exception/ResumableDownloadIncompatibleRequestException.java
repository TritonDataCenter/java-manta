/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

import com.joyent.manta.http.InputStreamContinuator;

/**
 * Exception indicating that the provided request cannot be reliable auto-resumed. This exception indicates a programmer
 * error since the request should've been checked for compatibility before attempting to be supplied to
 * {@link InputStreamContinuator}.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 * @since 3.2.3
 */
public class ResumableDownloadIncompatibleRequestException extends ResumableDownloadException {

    private static final long serialVersionUID = 7415723473743850334L;

    /**
     * Constructs an instance with the specified detail message and cause.
     *
     * @param message The detail message
     */
    public ResumableDownloadIncompatibleRequestException(final String message) {
        super(message);
    }


    /**
     * Constructs an instance with the specified detail message and cause.
     *
     * @param cause The cause
     */
    public ResumableDownloadIncompatibleRequestException(final Throwable cause) {
        super(cause);
    }

    /**
     * Constructs an instance with the specified detail message and cause.
     *
     * @param message The detail message
     * @param cause The cause
     */
    public ResumableDownloadIncompatibleRequestException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
