/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import com.joyent.manta.client.crypto.AesCipherDetailsFactory.CipherMode;
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
    public static final SupportedCipherDetails INSTANCE_128_BIT =
            AesCipherDetailsFactory.buildWith(CipherMode.CTR, 128);

    /**
     * Instance of AES192-CTR cipher.
     */
    public static final SupportedCipherDetails INSTANCE_192_BIT =
            AesCipherDetailsFactory.buildWith(CipherMode.CTR, 192);

    /**
     * Instance of AES256-CTR cipher.
     */
    public static final SupportedCipherDetails INSTANCE_256_BIT =
            AesCipherDetailsFactory.buildWith(CipherMode.CTR, 256);

    /**
     * Creates a new instance of a AES-CTR cipher for the static instance.
     *
     * @param keyLengthBits size of the private key - which determines the AES algorithm type
     */
    protected AesCtrCipherDetails(final int keyLengthBits) {
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
