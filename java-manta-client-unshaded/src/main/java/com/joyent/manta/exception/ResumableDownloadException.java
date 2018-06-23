/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

/**
 * Exception class for fatal errors which may occur while attempting to resumably (resiliently?) download an object.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 * @since 3.2.3
 */
public class ResumableDownloadException extends MantaIOException {

    private static final long serialVersionUID = -5972256969855482635L;

    /**
     * Constructs an instance with the specified detail message and cause.
     *
     * @param message The detail message
     */
    public ResumableDownloadException(final String message) {
        super(message);
    }

    /**
     * Constructs an instance with the specified detail message and cause.
     *
     * @param cause The cause
     */
    public ResumableDownloadException(final Throwable cause) {
        super(cause);
    }

    /**
     * Constructs an instance with the specified detail message and cause.
     *
     * @param message The detail message
     * @param cause The cause
     */
    public ResumableDownloadException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
