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
import javax.crypto.spec.GCMParameterSpec;

import java.security.NoSuchAlgorithmException;
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
    private static volatile AesGcmCipherDetails instance128Bit;

    /**
     * Instance of AES192-GCM cipher.
     */
    private static volatile AesGcmCipherDetails instance192Bit;

    /**
     * Instance of AES256-GCM cipher.
     */
    private static volatile AesGcmCipherDetails instance256Bit;

    /**
     * Method to retrieve AES-GCM-128 cipher details.
     *
     * @return AES-GCM-128 cipher details
     *
     * @throws NoSuchAlgorithmException if no provider implements AES-GCM-128
     */
    public static AesGcmCipherDetails aesGcm128() throws NoSuchAlgorithmException {

        // use double-checked lock to minimize parallel contention

        if (instance128Bit == null) {

            synchronized (AesGcmCipherDetails.class) {

                if (instance128Bit == null) {

                    instance128Bit = new AesGcmCipherDetails(128);
                }
            }
        }

        return instance128Bit;
    }

    /**
     * Method to retrieve AES-GCM-192 cipher details.
     *
     * @return AES-GCM-192 cipher details
     *
     * @throws NoSuchAlgorithmException if no provider implements AES-GCM-192
     */
    public static AesGcmCipherDetails aesGcm192() throws NoSuchAlgorithmException {

        // use double-checked lock to minimize parallel contention

        if (instance192Bit == null) {

            synchronized (AesGcmCipherDetails.class) {

                if (instance192Bit == null) {

                    instance192Bit = new AesGcmCipherDetails(192);
                }
            }
        }

        return instance192Bit;
    }

    /**
     * Method to retrieve AES-GCM-256 cipher details.
     *
     * @return AES-GCM-256 cipher details
     *
     * @throws NoSuchAlgorithmException if no provider implements AES-GCM-256
     */
    public static AesGcmCipherDetails aesGcm256() throws NoSuchAlgorithmException {

        // use double-checked lock to minimize parallel contention

        if (instance256Bit == null) {

            synchronized (AesGcmCipherDetails.class) {

                if (instance256Bit == null) {

                    instance256Bit = new AesGcmCipherDetails(256);
                }
            }
        }

        return instance256Bit;
    }

    /**
     * Creates a new instance of a AES-GCM cipher for the static instance.
     *
     * @param keyLengthBits size of the private key - which determines the AES algorithm type
     *
     * @throws NoSuchAlgorithmException if no provider implements the requested algorithm
     */
    private AesGcmCipherDetails(final int keyLengthBits) throws NoSuchAlgorithmException {
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
