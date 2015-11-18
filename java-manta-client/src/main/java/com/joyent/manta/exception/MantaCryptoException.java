/**
 * Copyright (c) 2013, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.exception;

/**
 * @author Yunong Xiao
 */
public class MantaCryptoException extends MantaClientException {

    private static final long serialVersionUID = -5734849034194919231L;

    /**
     * Empty constructor.
     */
    public MantaCryptoException() {
    }

    /**
     * @param message The exception message.
     */
    public MantaCryptoException(final String message) {
        super(message);
    }

    /**
     * @param cause The exception cause.
     */
    public MantaCryptoException(final Throwable cause) {
        super(cause);
    }

    /**
     * @param message The exception message.
     * @param cause The exception cause.
     */
    public MantaCryptoException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
