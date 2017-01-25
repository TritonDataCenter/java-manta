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
     * The largest ciphertext size allowed.
     */
    final long ciphertextMaxSize = (getMaximumPlaintextSizeInBytes() / getBlockSizeInBytes()) * getBlockSizeInBytes();

    /**
     * Creates a new instance of a AES-CBC cipher for the static instance.
     *
     * @param keyLengthBits size of the private key - which determines the AES algorithm type
     */
    private AesCbcCipherDetails(final int keyLengthBits) {
        super(keyLengthBits, "AES/CBC/PKCS5Padding", DEFAULT_HMAC_ALGORITHM);
    }

    @Override
    public long ciphertextSize(final long plaintextSize) {
        Validate.inclusiveBetween(0L, Long.MAX_VALUE, plaintextSize);

        if (plaintextSize <= 0) {
            return getBlockSizeInBytes() + getAuthenticationTagOrHmacLengthInBytes();
        }

        long totalBlocks = plaintextSize / getBlockSizeInBytes();

        return totalBlocks * getBlockSizeInBytes() + getBlockSizeInBytes()
                + getAuthenticationTagOrHmacLengthInBytes();
    }

    @Override
    public long plaintextSize(final long ciphertextSize) {
        Validate.inclusiveBetween(0L, Long.MAX_VALUE, ciphertextSize);

        if (ciphertextSize == getBlockSizeInBytes() + getAuthenticationTagOrHmacLengthInBytes()) {
            return 0L;
        }

        return ciphertextSize - getBlockSizeInBytes()
                - getAuthenticationTagOrHmacLengthInBytes();
    }

    @Override
    public boolean plaintextSizeCalculationIsAnEstimate() {
        return true;
    }

    @Override
    public long[] translateByteRange(final long startInclusive, final long endInclusive) {

        Validate.inclusiveBetween(0, ciphertextMaxSize, startInclusive,
                "Start position should be between 0 and 9223372036854775807");
        Validate.inclusiveBetween(-1, ciphertextMaxSize, endInclusive,
                "End position should be between -1 (undefined) and 9223372036854775807");

        long[] ranges = new long[4];

        final int blockSize = getBlockSizeInBytes();
        final long adjustedStart;
        final long adjustedEnd;

        if (startInclusive % blockSize == 0) {
            adjustedStart = startInclusive;
        } else {
            adjustedStart = (startInclusive / blockSize) * blockSize;
        }

        if (endInclusive % blockSize == 0) {
            adjustedEnd = endInclusive;
        } else {
            adjustedEnd = (endInclusive / blockSize) * blockSize + blockSize;
        }

        ranges[0] = adjustedStart;
        ranges[1] = 0L;
        ranges[2] = adjustedEnd;
        ranges[3] = 0L;

        return ranges;
    }
}
