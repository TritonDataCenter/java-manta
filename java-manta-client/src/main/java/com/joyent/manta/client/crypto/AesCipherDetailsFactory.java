/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import javax.crypto.Cipher;
import java.security.NoSuchAlgorithmException;

/**
 * Class for constructing AesCipherDetails which may or may not be useable in the current runtime.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celayac</a>
 * @since 3.0.0
 */
public final class AesCipherDetailsFactory {

    /**
     * Private constructor.
     */
    private AesCipherDetailsFactory() {
    }

    /**
     * Supported AES Cipher modes.
     */
    @SuppressWarnings("checkstyle:JavadocVariable")
    public enum CipherMode {
        CBC,
        CTR,
        GCM
    }

    /**
     * Default maximum key length.
     */
    static final int DEFAULT_MAX_KEY_LENGTH = 128;

    /**
     * @param mode cipher mode
     * @param requestedKeyLengthBits secret key length in bits
     *
     * @return static
     */
    static SupportedCipherDetails buildWith(final CipherMode mode, final int requestedKeyLengthBits) {

        if (!keyStrengthAllowedByRuntime(requestedKeyLengthBits)) {
            return new LocallyIllegalAesCipherDetails(requestedKeyLengthBits);
        }

        switch (mode) {
            case CBC:
                return new AesCbcCipherDetails(requestedKeyLengthBits);
            case CTR:
                return new AesCtrCipherDetails(requestedKeyLengthBits);
            case GCM:
                return new AesGcmCipherDetails(requestedKeyLengthBits);
            default:
                throw new Error("Invalid CipherMode provided when building AesCipherDetails: " + mode.toString());
        }
    }

    /**
     * Check if the requested key strength is permissible within the current runtime.
     *
     * @param requestedKeyLengthBits size of the secret key
     *
     * @return runtime legality of key length
     */
    private static boolean keyStrengthAllowedByRuntime(final int requestedKeyLengthBits) {
        int maxKeyLength;

        try {
            // TODO: potentially reuse {@code cipherAlgorithmJavaName} here?
            maxKeyLength = Cipher.getMaxAllowedKeyLength("AES");
            // will return DEFAULT_MAX_KEY_LENGTH if JCE missing, catch is for the compiler
        } catch (NoSuchAlgorithmException nsae) {
            maxKeyLength = DEFAULT_MAX_KEY_LENGTH;
        }

        return requestedKeyLengthBits <= maxKeyLength;
    }
}
