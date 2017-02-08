/*
 * Copyright (c) 2013-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

/**
 * Exception thrown when an operation that doesn't support authentication was
 * attempted when running in EncryptionAuthenticationMode.Mandatory mode.
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
