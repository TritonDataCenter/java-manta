/*
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.exception;

/**
 * Exception thrown when an operation that doesn't support authentication was
 * attempted when running in EncryptionObjectAuthenticationMode.Mandatory mode.
 *
 * @author Elijah Zupancic
 * @since 2.8.0
 */
public class UnauthenticatableOperationException
        extends MantaClientEncryptionException {
    private static final long serialVersionUID = 6129395920967997954L;

    /**
     * Default constructor.
     */
    public UnauthenticatableOperationException() {
    }

    /**
     * @param message The exception message.
     */
    public UnauthenticatableOperationException(final String message) {
        super(message);
    }

    /**
     * @param cause The exception cause.
     */
    public UnauthenticatableOperationException(final Throwable cause) {
        super(cause);
    }

    /**
     * @param message The exception message.
     * @param cause   The exception cause.
     */
    public UnauthenticatableOperationException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
