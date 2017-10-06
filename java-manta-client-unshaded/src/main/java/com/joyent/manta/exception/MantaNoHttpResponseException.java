/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

/**
 * Signals that the target server failed to respond with a valid HTTP response.
 *
 * This exception typically wraps {@link org.apache.http.NoHttpResponseException}.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.1.7
 */
public class MantaNoHttpResponseException extends MantaIOException {
    /**
     * Constructs an instance with {@code null}
     * as its error detail message.
     */
    public MantaNoHttpResponseException() {
    }

    /**
     * Constructs an instance with the specified detail message.
     *
     * @param message The detail message (which is saved for later retrieval
     *                by the {@link #getMessage()} method)
     */
    public MantaNoHttpResponseException(final String message) {
        super(message);
    }

    /**
     * Constructs an instance with the specified detail message
     * and cause.
     *
     * <p>Note that the detail message associated with {@code cause} is
     * <i>not</i> automatically incorporated into this exception's detail
     * message.</p>
     *
     * @param message The detail message (which is saved for later retrieval
     *                by the {@link #getMessage()} method)
     * @param cause The cause (which is saved for later retrieval by the
     *              {@link #getCause()} method).  (A null value is permitted,
     *              and indicates that the cause is nonexistent or unknown.)
     */
    public MantaNoHttpResponseException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs an instance with the specified cause and a
     * detail message of {@code (cause==null ? null : cause.toString())}
     * (which typically contains the class and detail message of {@code cause}).
     * This constructor is useful for IO exceptions that are little more
     * than wrappers for other throwables.
     *
     * @param cause The cause (which is saved for later retrieval by the
     *              {@link #getCause()} method).  (A null value is permitted,
     *              and indicates that the cause is nonexistent or unknown.)
     */
    public MantaNoHttpResponseException(final Throwable cause) {
        super(cause);
    }
}
