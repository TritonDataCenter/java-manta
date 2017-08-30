/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

/**
 * Exception class for errors related to encryption state memoization.
 */
public class MantaReflectionException extends MantaException {

    /**
     * @param message The exception message.
     */
    public MantaReflectionException(final String message) {
        super(message);
    }

    /**
     * @param cause The exception cause.
     */
    public MantaReflectionException(final Exception cause) {
        super(cause);
    }

    /**
     * @param message The exception message.
     * @param cause The exception cause.
     */
    public MantaReflectionException(final String message, final Exception cause) {
        super(message, cause);
    }
}
