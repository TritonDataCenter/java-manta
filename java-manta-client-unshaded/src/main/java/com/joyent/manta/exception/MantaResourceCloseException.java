/*
 * Copyright (c) 2019, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

/**
 * Exception wrapper class that wraps exceptions that occurred when calling
 * close() on a resource.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.3.1
 */
public class MantaResourceCloseException extends MantaIOException {
    private static final long serialVersionUID = -2587994161438179084L;

    /**
     * Constructs an instance with {@code null}
     * as its error detail message.
     */
    public MantaResourceCloseException() {
    }

    /**
     * Constructs an instance with the specified detail message.
     *
     * @param message
     *        The detail message (which is saved for later retrieval
     *        by the {@link #getMessage()} method)
     */
    public MantaResourceCloseException(final String message) {
        super(message);
    }

    /**
     * Constructs an instance with the specified detail message
     * and cause.
     *
     * <p> Note that the detail message associated with {@code cause} is
     * <i>not</i> automatically incorporated into this exception's detail
     * message.
     *
     * @param message
     *        The detail message (which is saved for later retrieval
     *        by the {@link #getMessage()} method)
     *
     * @param cause
     *        The cause (which is saved for later retrieval by the
     *        {@link #getCause()} method).  (A null value is permitted,
     *        and indicates that the cause is nonexistent or unknown.)
     */
    public MantaResourceCloseException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs an instance with the specified cause and a
     * detail message of {@code (cause==null ? null : cause.toString())}
     * (which typically contains the class and detail message of {@code cause}).
     * This constructor is useful for IO exceptions that are little more
     * than wrappers for other throwables.
     *
     * @param cause
     *        The cause (which is saved for later retrieval by the
     *        {@link #getCause()} method).  (A null value is permitted,
     *        and indicates that the cause is nonexistent or unknown.)
     */
    public MantaResourceCloseException(final Throwable cause) {
        super(cause);
    }
}
