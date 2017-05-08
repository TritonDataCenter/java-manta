/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import com.joyent.manta.util.LookupMap;
import com.joyent.manta.util.MantaUtils;

import java.security.NoSuchAlgorithmException;
import java.util.Map;

/**
 * Custom built {@link Map} implementation that supports case-sensitive and
 * case-insensitive operations for looking up {@link SupportedCipherDetails}
 * instances by algorithm name.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public final class SupportedCiphersLookupMap extends LookupMap<String, SupportedCipherDetails> {
    /**
     * Map of all of the ciphers supported by the SDK indexed by algorithm name.
     */
    public static final SupportedCiphersLookupMap INSTANCE = new SupportedCiphersLookupMap();

    /**
     * Package default constructor because interface is through {@link SupportedCipherDetails}.
     */
    private SupportedCiphersLookupMap() {

        super(supportedCipherMap());
    }

    /**
     * Method to construct a map of supported ciphers.
     *
     * @return map of supported ciphers
     */
    private static Map<String, SupportedCipherDetails> supportedCipherMap() {

        final Map<String, SupportedCipherDetails> map;

        try {

            map = MantaUtils.unmodifiableMap(
                        AesGcmCipherDetails.aesGcm128().getCipherId(), AesGcmCipherDetails.aesGcm128(),
                        AesGcmCipherDetails.aesGcm192().getCipherId(), AesGcmCipherDetails.aesGcm192(),
                        AesGcmCipherDetails.aesGcm256().getCipherId(), AesGcmCipherDetails.aesGcm256(),

                        AesCtrCipherDetails.aesCtr128().getCipherId(), AesCtrCipherDetails.aesCtr128(),
                        AesCtrCipherDetails.aesCtr192().getCipherId(), AesCtrCipherDetails.aesCtr192(),
                        AesCtrCipherDetails.aesCtr256().getCipherId(), AesCtrCipherDetails.aesCtr256(),

                        AesCbcCipherDetails.aesCbc128().getCipherId(), AesCbcCipherDetails.aesCbc128(),
                        AesCbcCipherDetails.aesCbc192().getCipherId(), AesCbcCipherDetails.aesCbc192(),
                        AesCbcCipherDetails.aesCbc256().getCipherId(), AesCbcCipherDetails.aesCbc256()
                );

        } catch (NoSuchAlgorithmException nsae) {

            final String msg = "unable to instantiate supported cipher details map";
            throw new Error(msg, nsae);
        }

        return map;
    }
}
