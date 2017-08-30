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
import javax.crypto.spec.GCMParameterSpec;
import java.security.spec.AlgorithmParameterSpec;

/**
 * Class that provides details about how the AES-GCM cipher's settings.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public final class AesGcmCipherDetails  extends AbstractAesCipherDetails {
    /**
     * The maximum number of 16 byte blocks that can be encrypted with a
     * AES GCM cipher: 2^32 - 2.
     */
    private static final long MAX_AES_GCM_BLOCKS = 4294967294L;

    /**
     * The maximum size in bytes allowed for a plain-text input to the AES-GCM
     * cipher.
     */
    private static final long MAX_PLAIN_TEXT_SIZE_BYTES = MAX_AES_GCM_BLOCKS << 4;

    /**
     * Instance of AES128-GCM cipher.
     */
    public static final SupportedCipherDetails INSTANCE_128_BIT =
            AesCipherDetailsFactory.buildWith(CipherMode.GCM, 128);

    /**
     * Instance of AES192-GCM cipher.
     */
    public static final SupportedCipherDetails INSTANCE_192_BIT =
            AesCipherDetailsFactory.buildWith(CipherMode.GCM, 192);

    /**
     * Instance of AES256-GCM cipher.
     */
    public static final SupportedCipherDetails INSTANCE_256_BIT =
            AesCipherDetailsFactory.buildWith(CipherMode.GCM, 256);

    /**
     * Creates a new instance of a AES-GCM cipher for the static instance.
     *
     * @param keyLengthBits size of the private key - which determines the AES algorithm type
     */
    protected AesGcmCipherDetails(final int keyLengthBits) {
        // Use 128-bit AEAD tag
        super(keyLengthBits, "AES/GCM/NoPadding", 16);
    }

    @Override
    public long getMaximumPlaintextSizeInBytes() {
        return MAX_PLAIN_TEXT_SIZE_BYTES;
    }

    @Override
    public AlgorithmParameterSpec getEncryptionParameterSpec(final byte[] iv) {
        Validate.notNull(iv, "Initialization vector must not be null");
        Validate.isTrue(iv.length == getIVLengthInBytes(),
                "Initialization vector has the wrong byte count [%d] "
                + "expected [%d] bytes", iv.length, getIVLengthInBytes());

        int tagSizeInBits = getAuthenticationTagOrHmacLengthInBytes() << 3;
        return new GCMParameterSpec(tagSizeInBits, iv);
    }

    @Override
    public long ciphertextSize(final long plaintextSize) {
        Validate.inclusiveBetween(0L, Long.MAX_VALUE, plaintextSize);
        return plaintextSize + getAuthenticationTagOrHmacLengthInBytes();
    }

    /**
     * Calculates the size of the plaintext data based on the ciphertext
     * size.
     *
     * @param ciphertextSize size of the ciphertext input
     * @return size of the plaintext output
     */
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
