/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

/**
 * Interface describing basic encrypted byte range attributes.
 *
 * @author <a href="https://github.com/uxcn">Jason Schulz</a>
 * @since 3.0.0
 */
public interface EncryptedRange {

    /**
     * Method to get decrypted offset for plaintext start.
     *
     * @return ciphertext decrypted offset for start bound
     */
    long getOffset();

    /**
     * Method to determine if range includes an <tt>mac</tt>.
     *
     * @return {@code true} if range includes an <tt>mac</tt>, {@code false}
     * otherwise
     */
    boolean includesMac();

}
