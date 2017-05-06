/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import org.testng.annotations.Test;

import javax.crypto.SecretKey;

@Test
public class AesCtrCipherDetailsTest extends AbstractCipherDetailsTest {

    private static final AesCtrCipherDetails AES_128_CTR = AesCtrCipherDetails.INSTANCE_128_BIT;

    private static final AesCtrCipherDetails AES_192_CTR = AesCtrCipherDetails.INSTANCE_192_BIT;

    private static final AesCtrCipherDetails AES_256_CTR = AesCtrCipherDetails.INSTANCE_256_BIT;

    private static final AesCtrCipherDetails DEFAULT_CIPHER = AES_256_CTR;

    private final long BLOCK_SIZE = DEFAULT_CIPHER.getBlockSizeInBytes();

    public void size1024bCalculationWorksRoundTripAes128() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AES_128_CTR, size);
    }

    public void size1024bCalculationWorksRoundTripAes192() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AES_192_CTR, size);
    }

    public void size1024bCalculationWorksRoundTripAes256() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AES_256_CTR, size);
    }

    public void size0bCalculationWorksRoundTripAes128() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AES_128_CTR, size);
    }

    public void size0bCalculationWorksRoundTripAes192() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AES_192_CTR, size);
    }

    public void size0bCalculationWorksRoundTripAes256() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AES_256_CTR, size);
    }

    public void size2009125bCalculationWorksRoundTripAes128() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AES_128_CTR, size);
    }

    public void size2009125bCalculationWorksRoundTripAes192() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AES_192_CTR, size);
    }

    public void size2009125bCalculationWorksRoundTripAes256() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AES_256_CTR, size);
    }

    public void ciphertextSizeCalculationWorksForAes128() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AES_128_CTR);
    }

    public void ciphertextSizeCalculationWorksForAes192() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AES_192_CTR);
    }

    public void ciphertextSizeCalculationWorksForAes256() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AES_256_CTR);
    }

    public void canQueryCiphertextByteRangeAes128() throws Exception {
        SecretKey secretKey = SecretKeyUtils.generate(AES_128_CTR);
        canRandomlyReadPlaintextPositionFromCiphertext(secretKey, AES_128_CTR);
    }

    public void canQueryCiphertextByteRangeAes192() throws Exception {
        SecretKey secretKey = SecretKeyUtils.generate(AES_192_CTR);
        canRandomlyReadPlaintextPositionFromCiphertext(secretKey, AES_192_CTR);
    }

    public void canQueryCiphertextByteRangeAes256() throws Exception {
        SecretKey secretKey = SecretKeyUtils.generate(AES_256_CTR);
        canRandomlyReadPlaintextPositionFromCiphertext(secretKey, AES_256_CTR);
    }

    public void canTranslatePlainToCipherStartZero() {

        testPlainToCipherStart(0, 0, DEFAULT_CIPHER);
    }

    public void canTranslatePlainToCipherStartPositiveFirstBlockA() {

        testPlainToCipherStart(1, 0, DEFAULT_CIPHER);
    }

    public void canTranslatePlainToCipherStartPositiveFirstBlockB() {

        testPlainToCipherStart(BLOCK_SIZE - 1, 0, DEFAULT_CIPHER);
    }

    public void canTranslatePlainToCipherStartPositiveSecondBlockA() {

        testPlainToCipherStart(BLOCK_SIZE, BLOCK_SIZE, DEFAULT_CIPHER);
    }

    public void canTranslatePlainToCipherStartPositiveSecondBlockB() {

        testPlainToCipherStart(BLOCK_SIZE + (BLOCK_SIZE - 1), BLOCK_SIZE, DEFAULT_CIPHER);
    }

    public void canTranslatePlainToCipherStartNegative() {

        testPlainToCipherStart(-1, -1, DEFAULT_CIPHER);
    }

    public void canTranslatePlainToCipherEnd() {

        testPlainToCipherEnd(1, 1, DEFAULT_CIPHER);
    }

    public void canGetBlockCipherOffsetBlockBoundaryPositive() {

        testPlainToCipherOffset(0, 0, DEFAULT_CIPHER);
    }

    public void canGetBlockCipherOffsetNonBlockBoundaryPositiveA() {

        testPlainToCipherOffset(1, 1, DEFAULT_CIPHER);
    }

    public void canGetBlockCipherOffsetNonBlockBoundaryPositiveB() {

        testPlainToCipherOffset(BLOCK_SIZE - 1, BLOCK_SIZE - 1, DEFAULT_CIPHER);
    }

    public void canGetBlockCipherOffsetNonBlockBoundaryNegativeA() {

        testPlainToCipherOffset(-1, BLOCK_SIZE - 1, DEFAULT_CIPHER);
    }

    public void canGetBlockCipherOffsetNonBlockBoundaryNegativeB() {

        testPlainToCipherOffset(-(BLOCK_SIZE - 1), 1, DEFAULT_CIPHER);
    }
}
