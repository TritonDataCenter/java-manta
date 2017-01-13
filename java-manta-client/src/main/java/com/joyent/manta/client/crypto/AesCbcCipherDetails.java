package com.joyent.manta.client.crypto;

import org.apache.commons.lang3.Validate;

/**
 * Class that provides details about how the AES-CBC cipher's settings.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public final class AesCbcCipherDetails extends AbstractAesCipherDetails {

    /**
     * Instance of AES128-CBC cipher.
     */
    public static final AesCbcCipherDetails INSTANCE_128_BIT = new AesCbcCipherDetails(128);

    /**
     * Instance of AES192-CBC cipher.
     */
    public static final AesCbcCipherDetails INSTANCE_192_BIT = new AesCbcCipherDetails(192);

    /**
     * Instance of AES256-CBC cipher.
     */
    public static final AesCbcCipherDetails INSTANCE_256_BIT = new AesCbcCipherDetails(256);

    /**
     * Creates a new instance of a AES-CBC cipher for the static instance.
     *
     * @param keyLengthBits size of the private key - which determines the AES algorithm type
     */
    private AesCbcCipherDetails(final int keyLengthBits) {
        super(keyLengthBits, "AES/CBC/PKCS5Padding", DEFAULT_HMAC_ALGORITHM);
    }

    @Override
    public long cipherTextSize(final long plainTextSize) {
        Validate.inclusiveBetween(0L, Long.MAX_VALUE, plainTextSize);

        if (plainTextSize <= 0) {
            return getBlockSizeInBytes() + getAuthenticationTagOrHmacLengthInBytes();
        }

        long totalBlocks = plainTextSize / getBlockSizeInBytes();

        return totalBlocks * getBlockSizeInBytes() + getBlockSizeInBytes()
                + getAuthenticationTagOrHmacLengthInBytes();
    }
}
