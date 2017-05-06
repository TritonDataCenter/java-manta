/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import org.apache.commons.lang3.Validate;

import javax.crypto.Cipher;

/**
 * Class that provides details about how the AES-CTR cipher's settings.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public final class AesCtrCipherDetails extends AbstractAesCipherDetails implements RandomAccessCipher {

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
    public long updateCipherToPosition(final Cipher cipher, final long position) {

        final int blockSize = getBlockSizeInBytes();
        final long block = position / blockSize;
        final long blockOffset = (position % blockSize);

        byte[] throwaway = new byte[blockSize];

        for (long i = 0; i < block; i++) {
            cipher.update(throwaway);
        }

        return blockOffset;
    }

    @Override
    public boolean supportsRandomAccess() {
        return true;
    }

    @Override
    public long plainToCipherStart(final long start) {

        final long s = start;
        final long bs = getBlockSizeInBytes();

        if (s < 0) {

            return s;

        } else {

            return bs * (s / bs);
        }

    }

    @Override
    public long plainToCipherEnd(final long end) {

        return end;
    }

    @Override
    public long plainToCipherOffset(final long pos) {

        final long bs = getBlockSizeInBytes();
        final long o = pos % bs;

        if (o < 0) {

            return bs + o;

        } else {

            return o;
        }

    }

}
