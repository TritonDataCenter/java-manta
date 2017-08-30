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
     * Maximum AES key length when Java Cryptography Extensions are missing.
     */
    static final int MAX_KEY_LENGTH_FALLBACK = 128;

    /**
     * Maximum key length allowed by current runtime.
     */
    static final int MAX_KEY_LENGTH_ALLOWED;

    static {
        int maxKeyLength;

        try {
            maxKeyLength = Cipher.getMaxAllowedKeyLength("AES");
            // will return MAX_KEY_LENGTH_FALLBACK if JCE missing, catch is for the compiler
        } catch (NoSuchAlgorithmException nsae) {
            maxKeyLength = MAX_KEY_LENGTH_FALLBACK;
        }

        MAX_KEY_LENGTH_ALLOWED = maxKeyLength;
    }

    /**
     * Builds a concrete subclass of AbstractAesCipherDetails, or a LocallyIllegalAesCipherDetails when the current
     * runtime lacks the required <a href="https://en.wikipedia.org/wiki/Java_Cryptography_Extension">Java Cryptography
     * Extensions</a>.
     *
     * @param mode cipher mode
     * @param requestedKeyLengthBits secret key length in bits
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
        return requestedKeyLengthBits <= MAX_KEY_LENGTH_ALLOWED;
    }
}
