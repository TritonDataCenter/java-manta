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
public class MantaMemoizationException extends MantaEncryptionException {
    /**
     * @param message The error message.
     */
    public MantaMemoizationException(final String message) {
        super(message);
    }

    /**
     * @param cause The cause of the exception.
     */
    public MantaMemoizationException(final Throwable cause) {
        super(cause);
    }

    /**
     * @param message The error message.
     * @param cause   The cause.
     */
    public MantaMemoizationException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
