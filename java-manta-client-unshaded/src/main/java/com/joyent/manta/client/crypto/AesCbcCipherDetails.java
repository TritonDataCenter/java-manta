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
 * Class that provides details about how the AES-CBC cipher's settings.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public final class AesCbcCipherDetails extends AbstractAesCipherDetails {

    /**
     * Instance of AES128-CBC cipher.
     */
    public static final SupportedCipherDetails INSTANCE_128_BIT =
            AesCipherDetailsFactory.buildWith(CipherMode.CBC, 128);

    /**
     * Instance of AES192-CBC cipher.
     */
    public static final SupportedCipherDetails INSTANCE_192_BIT =
            AesCipherDetailsFactory.buildWith(CipherMode.CBC, 192);

    /**
     * Instance of AES256-CBC cipher.
     */
    public static final SupportedCipherDetails INSTANCE_256_BIT =
            AesCipherDetailsFactory.buildWith(CipherMode.CBC, 256);

    /**
     * Creates a new instance of a AES-CBC cipher for the static instance.
     *
     * @param keyLengthBits size of the private key - which determines the AES algorithm type
     */
    protected AesCbcCipherDetails(final int keyLengthBits) {
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
