/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import org.apache.commons.lang3.Validate;

import java.security.NoSuchAlgorithmException;

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
    private static volatile AesCbcCipherDetails instance128Bit;

    /**
     * Instance of AES192-CBC cipher.
     */
    private static volatile AesCbcCipherDetails instance192Bit;

    /**
     * Instance of AES256-CBC cipher.
     */
    private static volatile AesCbcCipherDetails instance256Bit;

    /**
     * Method to retrieve AES-CBC-128 cipher details.
     *
     * @return AES-CBC-128 cipher details
     *
     * @throws NoSuchAlgorithmException if no provider implements AES-CBC-128
     */
    public static AesCbcCipherDetails aesCbc128() throws NoSuchAlgorithmException {

        // use double-checked lock to minimize parallel contention

        if (instance128Bit == null) {

            synchronized (AesCbcCipherDetails.class) {

                if (instance128Bit == null) {

                    instance128Bit = new AesCbcCipherDetails(128);
                }
            }
        }

        return instance128Bit;
    }

    /**
     * Method to retrieve AES-CBC-192 cipher details.
     *
     * @return AES-CBC-192 cipher details
     *
     * @throws NoSuchAlgorithmException if no provider implements AES-CBC-192
     */
    public static AesCbcCipherDetails aesCbc192() throws NoSuchAlgorithmException {

        // use double-checked lock to minimize parallel contention

        if (instance192Bit == null) {

            synchronized (AesCbcCipherDetails.class) {

                if (instance192Bit == null) {

                    instance192Bit = new AesCbcCipherDetails(192);
                }
            }
        }

        return instance192Bit;
    }

    /**
     * Method to retrieve AES-CBC-256 cipher details.
     *
     * @return AES-CBC-256 cipher details
     *
     * @throws NoSuchAlgorithmException if no provider implements AES-CBC-256
     */
    public static AesCbcCipherDetails aesCbc256() throws NoSuchAlgorithmException {

        // use double-checked lock to minimize parallel contention

        if (instance256Bit == null) {

            synchronized (AesCbcCipherDetails.class) {

                if (instance256Bit == null) {

                    instance256Bit = new AesCbcCipherDetails(256);
                }
            }
        }

        return instance256Bit;
    }

    /**
     * Creates a new instance of a AES-CBC cipher for the static instance.
     *
     * @param keyLengthBits size of the private key - which determines the AES algorithm type
     *
     * @throws NoSuchAlgorithmException if no provider implements the requested algorithm
     */
    private AesCbcCipherDetails(final int keyLengthBits) throws NoSuchAlgorithmException {
        super(keyLengthBits, "AES/CBC/PKCS5Padding", DEFAULT_HMAC_ALGORITHM);
    }

    @Override
    public long ciphertextSize(final long plaintextSize) {
        Validate.inclusiveBetween(0L, Long.MAX_VALUE, plaintextSize);
        int blockBytes = getBlockSizeInBytes();
        int tagOrHmacBytes = getAuthenticationTagOrHmacLengthInBytes();
        byte[] iv = this.getCipher().getIV();
        boolean hasIV = (iv != null && iv.length > 0);

        return calculateContentLength(plaintextSize, blockBytes, tagOrHmacBytes, hasIV);
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

    /**
     * Calculates the complete number of bytes for content including the hmac or tag if one exists.
     *
     * @param plaintextSize size in bytes of unencrypted body
     * @param blockBytes block size in bytes
     * @param tagOrHmacBytes number of bytes needed for the hmac or authentication tag
     * @param hasIV indicates if the instance has an initialization vector
     * @return calculated byte size of encrypted body with the hmac/authentication tag appended
     */
    static long calculateContentLength(final long plaintextSize, final int blockBytes, final int tagOrHmacBytes,
        final boolean hasIV) {

        if (plaintextSize <= 0) {
            return blockBytes + tagOrHmacBytes;
        }

        long calculatedContentLength = plaintextSize;
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

        // CBC requires an IV an extra block for chaining to work
        if (!hasIV && plaintextSize >= blockBytes) {
            final double blocks = Math.floor(plaintextSize / ((long) blockBytes));
            calculatedContentLength = (((long)blocks + 1) * blockBytes);
        }

        // Append tag or hmac to the end of the stream
        calculatedContentLength += tagOrHmacBytes;

        return calculatedContentLength;
    }
}
