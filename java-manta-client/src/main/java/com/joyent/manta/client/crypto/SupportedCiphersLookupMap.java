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
        super(runtimeCompatibleCiphers());
    }

    /**
     * Provide a map of legal ciphers for the current runtime.
     *
     * @return A plain Map for use with super() containing only the ciphers which are valid for the current runtime
     */
    private static Map<String, SupportedCipherDetails> runtimeCompatibleCiphers() {
        if (AesCipherDetailsFactory.MAX_KEY_LENGTH_FALLBACK == AesCipherDetailsFactory.MAX_KEY_LENGTH_ALLOWED) {
            // The max allowed cipher strength is unchanged, JCE is not installed

            return MantaUtils.unmodifiableMap(
                    AesGcmCipherDetails.INSTANCE_128_BIT.getCipherId(), AesGcmCipherDetails.INSTANCE_128_BIT,
                    AesCtrCipherDetails.INSTANCE_128_BIT.getCipherId(), AesCtrCipherDetails.INSTANCE_128_BIT,
                    AesCbcCipherDetails.INSTANCE_128_BIT.getCipherId(), AesCbcCipherDetails.INSTANCE_128_BIT
            );
        } else {
            return MantaUtils.unmodifiableMap(
                    AesGcmCipherDetails.INSTANCE_128_BIT.getCipherId(), AesGcmCipherDetails.INSTANCE_128_BIT,
                    AesGcmCipherDetails.INSTANCE_192_BIT.getCipherId(), AesGcmCipherDetails.INSTANCE_192_BIT,
                    AesGcmCipherDetails.INSTANCE_256_BIT.getCipherId(), AesGcmCipherDetails.INSTANCE_256_BIT,

                    AesCtrCipherDetails.INSTANCE_128_BIT.getCipherId(), AesCtrCipherDetails.INSTANCE_128_BIT,
                    AesCtrCipherDetails.INSTANCE_192_BIT.getCipherId(), AesCtrCipherDetails.INSTANCE_192_BIT,
                    AesCtrCipherDetails.INSTANCE_256_BIT.getCipherId(), AesCtrCipherDetails.INSTANCE_256_BIT,

                    AesCbcCipherDetails.INSTANCE_128_BIT.getCipherId(), AesCbcCipherDetails.INSTANCE_128_BIT,
                    AesCbcCipherDetails.INSTANCE_192_BIT.getCipherId(), AesCbcCipherDetails.INSTANCE_192_BIT,
                    AesCbcCipherDetails.INSTANCE_256_BIT.getCipherId(), AesCbcCipherDetails.INSTANCE_256_BIT
            );
        }
    }
}
