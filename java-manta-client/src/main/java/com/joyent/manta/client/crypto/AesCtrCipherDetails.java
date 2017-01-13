package com.joyent.manta.client.crypto;

import org.apache.commons.lang3.Validate;

/**
 * Class that provides details about how the AES-CTR cipher's settings.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public final class AesCtrCipherDetails extends AbstractAesCipherDetails {

    /**
     * Instance of AES128-CTR cipher.
     */
    public static final AesCtrCipherDetails INSTANCE_128_BIT = new AesCtrCipherDetails(128);

    /**
     * Instance of AES192-CTR cipher.
     */
    public static final AesCtrCipherDetails INSTANCE_192_BIT = new AesCtrCipherDetails(192);

    /**
     * Instance of AES256-CTR cipher.
     */
    public static final AesCtrCipherDetails INSTANCE_256_BIT = new AesCtrCipherDetails(256);

    /**
     * Creates a new instance of a AES-CTR cipher for the static instance.
     *
     * @param keyLengthBits size of the private key - which determines the AES algorithm type
     */
    private AesCtrCipherDetails(final int keyLengthBits) {
        super(keyLengthBits, "AES/CTR/NoPadding", DEFAULT_HMAC_ALGORITHM);    }


    @Override
    public long cipherTextSize(final long plainTextSize) {
        Validate.inclusiveBetween(0L, Long.MAX_VALUE, plainTextSize);
        return plainTextSize + getAuthenticationTagOrHmacLengthInBytes();
    }
}
