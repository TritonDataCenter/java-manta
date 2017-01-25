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
     * The largest ciphertext size allowed.
     */
    final long ciphertextMaxSize = (getMaximumPlaintextSizeInBytes() / getBlockSizeInBytes()) * getBlockSizeInBytes();

    /**
     * Creates a new instance of a AES-CTR cipher for the static instance.
     *
     * @param keyLengthBits size of the private key - which determines the AES algorithm type
     */
    private AesCtrCipherDetails(final int keyLengthBits) {
        super(keyLengthBits, "AES/CTR/NoPadding", DEFAULT_HMAC_ALGORITHM);    }


    @Override
    public long ciphertextSize(final long plaintextSize) {
        Validate.inclusiveBetween(0L, Long.MAX_VALUE, plaintextSize);
        return plaintextSize + getAuthenticationTagOrHmacLengthInBytes();
    }

    @Override
    public long plaintextSize(final long ciphertextSize) {
        Validate.inclusiveBetween(0L, Long.MAX_VALUE, ciphertextSize);
        return ciphertextSize - getAuthenticationTagOrHmacLengthInBytes();
    }

    @Override
    public boolean plaintextSizeCalculationIsAnEstimate() {
        return false;
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
        final long plaintextStartAdjustment;
        final long adjustedEnd;
        final long plaintextEndLength;

        if (startInclusive % blockSize == 0) {
            adjustedStart = startInclusive;
            plaintextStartAdjustment = 0L;
        } else {
            final long blockOverlap = (startInclusive / blockSize);
            adjustedStart = blockOverlap * blockSize;
            plaintextStartAdjustment = startInclusive - adjustedStart;
        }

        if (endInclusive % blockSize == 0) {
            adjustedEnd = endInclusive;
        } else {
            adjustedEnd = (endInclusive / blockSize) * blockSize + blockSize;
        }

        plaintextEndLength = endInclusive - startInclusive;

        ranges[0] = adjustedStart;
        ranges[1] = plaintextStartAdjustment;
        ranges[2] = adjustedEnd;
        ranges[3] = plaintextEndLength;

        return ranges;
    }
}
