/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

/**
 * General exception type for errors relating to multipart uploads.
 *
 * @since 2.5.0
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaMultipartException extends MantaClientException {

    private static final long serialVersionUID = -1931282527258322479L;

    /**
     * Create an empty exception.
     */
    public MantaMultipartException() {
    }

    /**
     * @param message The error message.
     */
    public MantaMultipartException(final String message) {
        super(message);
    }

    /**
     * @param cause The cause of the exception.
     */
    public MantaMultipartException(final Throwable cause) {
        super(cause);
    }

    /**
     * @param message The error message.
     * @param cause The cause.
     */
    public MantaMultipartException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
