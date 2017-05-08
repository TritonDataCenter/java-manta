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
 * Class that provides details about how the AES-CTR cipher's settings.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public final class AesCtrCipherDetails extends AbstractAesCipherDetails {

    /**
     * Instance of AES128-CTR cipher.
     */
    private static volatile AesCtrCipherDetails instance128Bit;

    /**
     * Instance of AES192-CTR cipher.
     */
    private static volatile AesCtrCipherDetails instance192Bit;

    /**
     * Instance of AES256-CTR cipher.
     */
    private static volatile AesCtrCipherDetails instance256Bit;

    /**
     * Method to retrieve AES-CTR-128 cipher details.
     *
     * @return AES-CTR-128 cipher details
     *
     * @throws NoSuchAlgorithmException if no provider implements AES-CTR-128
     */
    public static AesCtrCipherDetails aesCtr128() throws NoSuchAlgorithmException {

        // use double-checked lock to minimize parallel contention

        if (instance128Bit == null) {

            synchronized (AesCtrCipherDetails.class) {

                if (instance128Bit == null) {

                    instance128Bit = new AesCtrCipherDetails(128);
                }
            }
        }

        return instance128Bit;
    }

    /**
     * Method to retrieve AES-CTR-192 cipher details.
     *
     * @return AES-CTR-192 cipher details
     *
     * @throws NoSuchAlgorithmException if no provider implements AES-CTR-192
     */
    public static AesCtrCipherDetails aesCtr192() throws NoSuchAlgorithmException {

        // use double-checked lock to minimize parallel contention

        if (instance192Bit == null) {

            synchronized (AesCtrCipherDetails.class) {

                if (instance192Bit == null) {

                    instance192Bit = new AesCtrCipherDetails(192);
                }
            }
        }

        return instance192Bit;
    }

    /**
     * Method to retrieve AES-CTR-256 cipher details.
     *
     * @return AES-CTR-256 cipher details
     *
     * @throws NoSuchAlgorithmException if no provider implements AES-CTR-256
     */
    public static AesCtrCipherDetails aesCtr256() throws NoSuchAlgorithmException {

        // use double-checked lock to minimize parallel contention

        if (instance256Bit == null) {

            synchronized (AesCtrCipherDetails.class) {

                if (instance256Bit == null) {

                    instance256Bit = new AesCtrCipherDetails(256);
                }
            }
        }

        return instance256Bit;
    }

    /**
     * Creates a new instance of a AES-CTR cipher for the static instance.
     *
     * @param keyLengthBits size of the private key - which determines the AES algorithm type
     *
     * @throws NoSuchAlgorithmException if no provider implements the requested algorithm
     */
    private AesCtrCipherDetails(final int keyLengthBits) throws NoSuchAlgorithmException {
        super(keyLengthBits, "AES/CTR/NoPadding", DEFAULT_HMAC_ALGORITHM);
    }

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

        if (startInclusive > endInclusive) {
            String msg = "Start position must be precede end position (startInclusive=" + startInclusive
                    + ", endInclusive=" + endInclusive + ")";
            throw new IllegalArgumentException(msg);
        }

        if (startInclusive < 0) {
            String msg = "Start position must be zero or higher (startInclusive=" + startInclusive + ")";
            throw new IllegalArgumentException(msg);
        }

        if (startInclusive >= plaintextMax) {
            String msg = "Start position must be less than maximum plaintext size (startInclusive="
                    + startInclusive + ", plaintextMax=" + plaintextMax + ")";
            throw new IllegalArgumentException(msg);
        }

        if (endInclusive < 0) {
            String msg = "End position must be zero or higher (endInclusive=" + endInclusive + ")";
            throw new IllegalArgumentException(msg);
        }

        if (endInclusive >= plaintextMax) {
            String msg = "End position must be less than maximum plaintext size (endInclusive=" + endInclusive
                    + ", plaintextMax=" + plaintextMax + ")";
            throw new IllegalArgumentException(msg);
        }

        final int blockSize = getBlockSizeInBytes();

        // How many bytes do we offset in the first block?
        final long plaintextBytesToSkipInitially = startInclusive % blockSize;

        // Get the ciphertext block index for the start byte (zero based)
        final long startingBlockNumberInclusive = startInclusive / blockSize;

        // Get the ciphertext byte position for the start block (block size bytes per block)
        final long ciphertextStartPositionInclusive = startingBlockNumberInclusive * blockSize;

        // Get the ciphertext block index for the end byte (zero based)
        final long endingBlockNumberInclusive = endInclusive / blockSize;

        // Get the ciphertext byte position for the start block (next block subtract a byte)
        final long ciphertextEndPositionInclusive = ((endingBlockNumberInclusive + 1) * blockSize) - 1;

        // Compute plaintext length (byte indices are zero based)
        final long lengthOfPlaintextIncludingSkipBytes = (endInclusive - startInclusive)
                + (plaintextBytesToSkipInitially + 1);

        return new ByteRangeConversion(ciphertextStartPositionInclusive, plaintextBytesToSkipInitially,
                ciphertextEndPositionInclusive, lengthOfPlaintextIncludingSkipBytes, startingBlockNumberInclusive);
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
