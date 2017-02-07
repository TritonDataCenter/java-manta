package com.joyent.manta.client.crypto;

import org.apache.commons.lang3.Validate;

import javax.crypto.Cipher;

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
    private final long ciphertextMaxSize = (getMaximumPlaintextSizeInBytes() / getBlockSizeInBytes())
            * getBlockSizeInBytes();

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
    public ByteRangeConversion translateByteRange(final long startInclusive, final long endInclusive) {
        final long plaintextMax = getMaximumPlaintextSizeInBytes();

        if (startInclusive < 0) {
            String msg = String.format("Start position must be zero or higher. Actually: %d",
                    startInclusive);
            throw new IllegalArgumentException(msg);
        }

        if (startInclusive > plaintextMax) {
            String msg = String.format("Start position must be less than maximum "
                            + "ciphertext size [%d]. Actually: %d",
                    plaintextMax, startInclusive);
            throw new IllegalArgumentException(msg);
        }

        if (endInclusive < 0) {
            String msg = String.format("End position must be zero or higher. Actually: %d",
                    endInclusive);
            throw new IllegalArgumentException(msg);
        }

        if (endInclusive > plaintextMax) {
            String msg = String.format("End position must be less than maximum "
                            + "ciphertext size [%d]. Actually: %d",
                    plaintextMax, endInclusive);
            throw new IllegalArgumentException(msg);
        }

        final int blockSize = getBlockSizeInBytes();
        long adjustedStart;
        long adjustedEnd;
        long plaintextEndLength;

        // How many bytes do we offset in the first block
        long plaintextStartAdjustment = startInclusive % blockSize;

        // The first block is considered block 0
        double startBlockNumber = Math.floor(startInclusive / blockSize);

        // Content-Range: bytes=adjustedStart-adjustedEnd
        adjustedStart = ((long) startBlockNumber) * blockSize;

        // Get the full block for the end range, plus an additional block
        double endBlockNumber = Math.ceil(endInclusive / blockSize) + 1;

        adjustedEnd = (((long) endBlockNumber) * blockSize) + 1;

        // We want to read a single byte, subtract 1 from adjustedEnd, which assumed 2 or more bytes are read
        if (startInclusive == endInclusive) {
            adjustedEnd--;
        }

        plaintextEndLength = (endInclusive - startInclusive) + 1;

        return new ByteRangeConversion(adjustedStart, plaintextStartAdjustment,
                adjustedEnd, plaintextEndLength, (long)startBlockNumber);
    }

    @Override
    public long updateCipherToPosition(final Cipher cipher, final long position) {
        final int blockSize = getBlockSizeInBytes();
        final long block = position / blockSize;
        final long skip = (position % blockSize);

        byte[] throwaway = new byte[blockSize];
        for (long i = 0; i < block; i++) {
            cipher.update(throwaway);
        }

        return skip;
    }

    @Override
    public boolean supportsRandomAccess() {
        return true;
    }
}
