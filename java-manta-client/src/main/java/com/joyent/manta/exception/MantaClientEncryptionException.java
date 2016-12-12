/*
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.exception;

/**
 * Root exception type for all client-side encryption exceptions.
 *
 * @author Elijah Zupancic
 * @since 2.8.0
 */
public class MantaClientEncryptionException extends MantaException {
    private static final long serialVersionUID = -1687775226513239704L;

    /**
     * Default constructor.
     */
    public MantaClientEncryptionException() {
    }

    /**
     * @param message The exception message.
     */
    public MantaClientEncryptionException(final String message) {
        super(message);
    }

    /**
     * @param cause The exception cause.
     */
    public MantaClientEncryptionException(final Throwable cause) {
        super(cause);
    }

    /**
     * @param message The exception message.
     * @param cause   The exception cause.
     */
    public MantaClientEncryptionException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
