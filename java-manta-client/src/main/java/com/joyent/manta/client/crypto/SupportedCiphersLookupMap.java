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
     * Package default constructor because interface is through {@link SupportedCipherDetails}.
     */
    SupportedCiphersLookupMap() {
        super(MantaUtils.unmodifiableMap(
                AesGcmCipherDetails.INSTANCE.getCipherAlgorithm(), AesGcmCipherDetails.INSTANCE,
                AesCtrCipherDetails.INSTANCE.getCipherAlgorithm(), AesCtrCipherDetails.INSTANCE,
                AesCbcCipherDetails.INSTANCE.getCipherAlgorithm(), AesCbcCipherDetails.INSTANCE
        ));
    }
}
