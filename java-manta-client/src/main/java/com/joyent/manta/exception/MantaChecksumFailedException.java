/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

import com.joyent.manta.http.HttpHelper;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;

/**
 * Exception thrown when the checksum for a file that is being uploaded doesn't
 * match the server-side generated checksum.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class MantaChecksumFailedException extends MantaIOException {
    private static final long serialVersionUID = -3056054264146136478L;

    /**
     * Constructs an instance with {@code null}
     * as its error detail message.
     */
    public MantaChecksumFailedException() {
    }

    /**
     * Constructs an instance with the specified detail message.
     *
     * @param message The detail message (which is saved for later retrieval
     *                by the {@link #getMessage()} method)
     */
    public MantaChecksumFailedException(final String message) {
        super(message);
    }

    /**
     * <p>Constructs an instance with the specified detail message
     * and cause.</p>
     *
     * <p>Note that the detail message associated with {@code cause} is
     * <i>not</i> automatically incorporated into this exception's detail
     * message.</p>
     *
     * @param message The detail message (which is saved for later retrieval
     *                by the {@link #getMessage()} method)
     * @param cause   The cause (which is saved for later retrieval by the
     *                {@link #getCause()} method).  (A null value is permitted,
     */
    public MantaChecksumFailedException(final String message, final Throwable cause) {
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
    public MantaChecksumFailedException(final Throwable cause) {
        super(cause);
    }

    /**
     * Builds a client exception object that is annotated with all of the
     * relevant request and response debug information.
     *
     * @param message  The detail message (which is saved for later retrieval
     *                 by the {@link #getMessage()} method)
     * @param request  HTTP request object
     * @param response HTTP response object
     */
    public MantaChecksumFailedException(final String message, final HttpRequest request, final HttpResponse response) {
        super(message);
        HttpHelper.annotateContextedException(this, request, response);
    }

}
