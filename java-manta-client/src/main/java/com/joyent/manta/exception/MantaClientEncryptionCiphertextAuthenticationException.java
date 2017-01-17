/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.exception;

/**
 * Exception representing a failure to authenticate (validate the authenticity)
 * of an object's binary ciphertext against a checksum.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class MantaClientEncryptionCiphertextAuthenticationException extends MantaClientEncryptionException {
    /**
     * Default message displayed when exception is thrown.
     */
    private static final String DEFAULT_MESSAGE = "Unable to authenticate object's ciphertext. It failed "
            + "the authenticity checksum. Check to see that the object's "
            + "binary data hasn't been modified.";

    /**
     * Default constructor.
     */
    public MantaClientEncryptionCiphertextAuthenticationException() {
        super(DEFAULT_MESSAGE);
    }

    /**
     * @param message The exception message.
     */
    public MantaClientEncryptionCiphertextAuthenticationException(final String message) {
        super(message + " " + message);
    }

    /**
     * @param cause The exception cause.
     */
    public MantaClientEncryptionCiphertextAuthenticationException(final Throwable cause) {
        super(DEFAULT_MESSAGE, cause);
    }

    /**
     * @param message The exception message.
     * @param cause   The exception cause.
     */
    public MantaClientEncryptionCiphertextAuthenticationException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
