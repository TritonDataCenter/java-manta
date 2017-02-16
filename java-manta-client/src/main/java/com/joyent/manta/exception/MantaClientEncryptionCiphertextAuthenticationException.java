/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

/**
 * Exception representing a failure to authenticate (validate the authenticity)
 * of an object's binary ciphertext against a checksum.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class MantaClientEncryptionCiphertextAuthenticationException extends MantaClientEncryptionException {
    private static final long serialVersionUID = 6663027525586063919L;

    /**
     * Default message displayed when exception is thrown.
     */
    private static final String DEFAULT_MESSAGE = "Unable to authenticate object's ciphertext. It failed "
            + "the authenticity checksum. Check to see that the object's "
            + "binary data hasn't been modified.";


    /**
     * Default constructor.
     */
    public MantaClientEncryptionCiphertextAuthenticationException() {
        super(DEFAULT_MESSAGE);
    }

    /**
     * @param message The exception message.
     */
    public MantaClientEncryptionCiphertextAuthenticationException(final String message) {
        super(message);
    }

    /**
     * @param cause The exception cause.
     */
    public MantaClientEncryptionCiphertextAuthenticationException(final Throwable cause) {
        super(DEFAULT_MESSAGE, cause);
    }

    /**
     * @param message The exception message.
     * @param cause   The exception cause.
     */
    public MantaClientEncryptionCiphertextAuthenticationException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
