/**
 * Copyright (c) 2013, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.exception;

/**
 * The base MantaException class.
 * @author Yunong Xiao
 */
public class MantaException extends RuntimeException {

    private static final long serialVersionUID = 146894136987570504L;

    /**
     * Default constructor.
     */
    public MantaException() {
    }

    /**
     * @param message The exception message.
     */
    public MantaException(final String message) {
        super(message);
    }

    /**
     * @param cause The exception cause.
     */
    public MantaException(final Throwable cause) {
        super(cause);
    }

    /**
     * @param message The exception message.
     * @param cause The exception cause.
     */
    public MantaException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
