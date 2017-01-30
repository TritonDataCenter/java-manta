package com.joyent.manta.client.crypto;

import org.apache.commons.lang3.Validate;

import javax.crypto.Cipher;

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
    public long ciphertextSize(final long plaintextSize) {
        Validate.inclusiveBetween(0L, Long.MAX_VALUE, plaintextSize);
        int blockBytes = getBlockSizeInBytes();
        int tagOrHmacBytes = getAuthenticationTagOrHmacLengthInBytes();

        if (plaintextSize <= 0) {
            return blockBytes + tagOrHmacBytes;
        }

        long calculatedContentLength = 0L;
        long padding = 0L;
        if (plaintextSize > blockBytes) {
            padding = plaintextSize % blockBytes;
        } else {
            calculatedContentLength = blockBytes;
        }

        // e.g. content is 20 bytes, block is 16, padding is 4, result = 32
        if (padding > 0) {
            calculatedContentLength = (plaintextSize - padding) + blockBytes;
        }

        byte[] iv = this.getCipher().getIV();
        // CBC requires an IV an extra block for chaining to work
        if ((iv == null || iv.length == 0) && plaintextSize >= blockBytes) {
            final double blocks = Math.floor(plaintextSize / ((long) blockBytes));
            calculatedContentLength = (((long)blocks + 1) * blockBytes);
        }


        // Append tag or hmac to the end of the stream
        calculatedContentLength += tagOrHmacBytes;

        return calculatedContentLength;
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
    public ByteRangeConversion translateByteRange(final long startInclusive, final long endInclusive) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public long updateCipherToPosition(final Cipher cipher, final long position) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public boolean supportsRandomAccess() {
        return false;
    }
}
