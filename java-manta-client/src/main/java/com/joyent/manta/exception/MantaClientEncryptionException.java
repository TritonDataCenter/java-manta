/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

/**
 * Root exception type for all client-side encryption exceptions.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class MantaClientEncryptionException extends MantaEncryptionException {
    private static final long serialVersionUID = -1687775226513239704L;

    /**
     * Default constructor.
     */
    public MantaClientEncryptionException() {
    }

    /**
     * @param message The exception message.
     */
    public MantaClientEncryptionException(final String message) {
        super(message);
    }

    /**
     * @param cause The exception cause.
     */
    public MantaClientEncryptionException(final Throwable cause) {
        super(cause);
    }

    /**
     * @param message The exception message.
     * @param cause   The exception cause.
     */
    public MantaClientEncryptionException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
