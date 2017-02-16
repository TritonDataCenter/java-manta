/*
 * Copyright (c) 2013-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

/**
 * General exception class for Manta client exceptions.
 *
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
