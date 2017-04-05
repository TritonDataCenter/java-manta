/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

/**
 * Root exception type for all encryption exceptions.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class MantaEncryptionException extends MantaException {
    private static final long serialVersionUID = -1210560907538988566L;

    /**
     * Default constructor.
     */
    public MantaEncryptionException() {
    }

    /**
     * @param message The exception message.
     */
    public MantaEncryptionException(final String message) {
        super(message);
    }

    /**
     * @param cause The exception cause.
     */
    public MantaEncryptionException(final Throwable cause) {
        super(cause);
    }

    /**
     * @param message The exception message.
     * @param cause   The exception cause.
     */
    public MantaEncryptionException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
