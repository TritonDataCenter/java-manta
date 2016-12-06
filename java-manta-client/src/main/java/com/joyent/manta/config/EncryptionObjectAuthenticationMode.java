/**
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.config;

/**
 * Enum specifying the value object encryption authentication modes.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 2.8.0
 */
public enum EncryptionObjectAuthenticationMode {
    /**
     * Optional mode allows for the selective skipping of authentication if it is
     * incompatible with an operation being attempted like HTTP range requests.
     */
    Optional,

    /**
     * Mandatory mode will require that every operation authenticates ciphertext
     * and operations that are incompatible will fail with a
     * {@link com.joyent.manta.exception.UnauthenticatableOperationException}
     * exception.
     */
    Mandatory;

    /**
     * The default encryption object authentication mode (Mandatory).
     */
    public static final EncryptionObjectAuthenticationMode DEFAULT_MODE = Mandatory;
}
