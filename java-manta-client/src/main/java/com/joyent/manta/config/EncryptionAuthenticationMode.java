/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.config;

/**
 * Enum specifying the value object encryption authentication modes.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 2.8.0
 */
public enum EncryptionAuthenticationMode {
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
    public static final EncryptionAuthenticationMode DEFAULT_MODE = Mandatory;
}
