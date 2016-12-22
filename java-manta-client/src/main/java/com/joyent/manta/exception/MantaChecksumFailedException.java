/*
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.exception;

/**
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
    public MantaChecksumFailedException(String message) {
        super(message);
    }

    /**
     * Constructs an instance with the specified detail message
     * and cause.
     * <p>
     * <p> Note that the detail message associated with {@code cause} is
     * <i>not</i> automatically incorporated into this exception's detail
     * message.
     *
     * @param message The detail message (which is saved for later retrieval
     *                by the {@link #getMessage()} method)
     * @param cause   The cause (which is saved for later retrieval by the
     *                {@link #getCause()} method).  (A null value is permitted,
     */
    public MantaChecksumFailedException(String message, Throwable cause) {
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
    public MantaChecksumFailedException(Throwable cause) {
        super(cause);
    }
}
