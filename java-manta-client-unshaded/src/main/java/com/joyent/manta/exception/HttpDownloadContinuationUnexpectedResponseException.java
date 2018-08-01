/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

/**
 * Exception signaling that a resumed download request cannot be continued because the response was invalid.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 * @since 3.2.3
 */
public class HttpDownloadContinuationUnexpectedResponseException extends HttpDownloadContinuationException {

    private static final long serialVersionUID = 123778476650917899L;

    /**
     * Constructs an instance with the specified detail message and cause.
     *
     * @param message The detail message
     */
    public HttpDownloadContinuationUnexpectedResponseException(final String message) {
        super(message);
    }

    /**
     * Constructs an instance with the specified detail message and cause.
     *
     * @param cause The cause
     */
    public HttpDownloadContinuationUnexpectedResponseException(final Throwable cause) {
        super(cause);
    }

    /**
     * Constructs an instance with the specified detail message and cause.
     *
     * @param message The detail message
     * @param cause The cause
     */
    public HttpDownloadContinuationUnexpectedResponseException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
