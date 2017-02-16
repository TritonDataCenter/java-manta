/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.serialization;

import com.joyent.manta.exception.MantaClientException;

/**
 * Exception class for errors relating to serialization.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class MantaClientSerializationException extends MantaClientException {
    private static final long serialVersionUID = 2881593409681385799L;

    /**
     * Create an empty exception.
     */
    public MantaClientSerializationException() {
    }

    /**
     * @param message The error message.
     */
    public MantaClientSerializationException(final String message) {
        super(message);
    }

    /**
     * @param cause The cause of the exception.
     */
    public MantaClientSerializationException(final Throwable cause) {
        super(cause);
    }

    /**
     * @param message The error message.
     * @param cause   The cause.
     */
    public MantaClientSerializationException(final String message,
                                             final Throwable cause) {
        super(message, cause);
    }
}
