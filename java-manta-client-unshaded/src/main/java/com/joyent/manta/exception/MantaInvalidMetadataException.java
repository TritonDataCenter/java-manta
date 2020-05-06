/*
 * Copyright (c) 2020, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

/**
 * Exception while adding metadata.
 *
 * @author <a href="https://github.com/nairashwin952013">Ashwin A Nair</a>
 * @since 3.5.0
 */
public class MantaInvalidMetadataException extends MantaClientException {

    private static final long serialVersionUID = 3946821793051523289L;

    /**
     * @param message The error message.
     */
    @SuppressWarnings("unused")
    public MantaInvalidMetadataException(final String message) {
        super(message);
    }

    /**
     * @param cause The cause of the exception.
     */
    @SuppressWarnings("unused")
    public MantaInvalidMetadataException(final Throwable cause) {
        super(cause);
    }

    /**
     * @param message The error message.
     * @param cause   The cause.
     */
    public MantaInvalidMetadataException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
