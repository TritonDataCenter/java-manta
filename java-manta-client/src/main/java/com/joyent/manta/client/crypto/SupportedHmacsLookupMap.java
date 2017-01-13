package com.joyent.manta.client.crypto;

import com.joyent.manta.exception.MantaClientEncryptionException;
import com.joyent.manta.util.LookupMap;
import com.joyent.manta.util.MantaUtils;

import javax.crypto.Mac;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Custom built {@link Map} implementation that supports case-sensitive and
 * case-insensitive operations for looking up {@link javax.crypto.Mac}
 * instances by algorithm name.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public final class SupportedHmacsLookupMap extends LookupMap<String, Supplier<Mac>> {
    /**
     * Map of all of the HMACs supported by the SDK indexed by algorithm name.
     */
    public static final SupportedHmacsLookupMap INSTANCE = new SupportedHmacsLookupMap();

    /**
     * Package default constructor because interface is through {@link SupportedCipherDetails}.
     */
    private SupportedHmacsLookupMap() {
        super(MantaUtils.unmodifiableMap(
                "HmacMD5", hmacSupplierByName("HmacMD5"),
                "HmacSHA1", hmacSupplierByName("HmacSHA1"),
                "HmacSHA256", hmacSupplierByName("HmacSHA256"),
                "HmacSHA512", hmacSupplierByName("HmacSHA512"))
        );
    }

    /**
     * Wraps a getInstance call as a {@link Supplier} so that we can return a
     * new HMAC instance for every value of this map.
     *
     * @param algorithm algorithm to instantiate HMAC instance as
     * @return supplier wrapping getInstance call to get HMAC instance
     */
    private static Supplier<Mac> hmacSupplierByName(final String algorithm) {
        return () -> {
            try {
                return Mac.getInstance(algorithm);
            } catch (NoSuchAlgorithmException e) {
                String msg = String.format("Hmac algorithm [%s] not supported");
                throw new MantaClientEncryptionException(msg, e);
            }
        };
    }
}
