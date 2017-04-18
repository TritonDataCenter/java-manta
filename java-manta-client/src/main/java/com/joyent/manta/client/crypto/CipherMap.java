/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

/**
 * Interface describing a plaintext to ciphertext byte position map.
 *
 * @author <a href="https://github.com/uxcn">Jason Schulz</a>
 * @since 3.0.0
 */
public interface CipherMap {

    /**
     * Method to map a plaintext start byte position to ciphertext start byte
     * position.
     *
     * @param start plaintext start position
     *
     * @return ciphertext start
     */
    long plainToCipherStart(long start);

    /**
     * Method to map a plaintext end byte position to ciphertext end byte
     * position.
     *
     * @param end plaintext position
     *
     * @return ciphertext end
     */
    long plainToCipherEnd(long end);

    /**
     * Method to map plaintext byte position to offset in decrypted ciphertext.
     *
     * @param pos plaintext position
     *
     * @return decrypted ciphertext offset
     */
    long plainToCipherOffset(long pos);

}
