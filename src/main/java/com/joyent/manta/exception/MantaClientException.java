/**
 * Copyright (c) 2013, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.exception;

/**
 * @author Yunong Xiao
 */
public class MantaClientException extends MantaException {

    private static final long serialVersionUID = 4004550753800130185L;

    /**
     * Create an empty exception.
     */
    public MantaClientException() {
    }

    /**
     * @param message The error message.
     */
    public MantaClientException(final String message) {
        super(message);
    }

    /**
     * @param cause The cause of the exception.
     */
    public MantaClientException(final Throwable cause) {
        super(cause);
    }

    /**
     * @param message The error message.
     * @param cause The cause.
     */
    public MantaClientException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
