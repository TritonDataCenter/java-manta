/**
 * Copyright (c) 2013, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.exception;

/**
 * @author Yunong Xiao
 */
public class MantaClientException extends MantaException {

    private static final long serialVersionUID = 4004550753800130185L;

    public MantaClientException() {
    }

    /**
     * @param message
     */
    public MantaClientException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public MantaClientException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public MantaClientException(String message, Throwable cause) {
        super(message, cause);
    }

}
