/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import javax.crypto.SecretKey;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class AesCbcCipherDetailsTest extends AbstractCipherDetailsTest {

    private static final AesCbcCipherDetails AES_128_CBC = AesCbcCipherDetails.INSTANCE_128_BIT;

    private static final AesCbcCipherDetails AES_192_CBC = AesCbcCipherDetails.INSTANCE_192_BIT;

    private static final AesCbcCipherDetails AES_256_CBC = AesCbcCipherDetails.INSTANCE_256_BIT;

    private static final AesCbcCipherDetails DEFAULT_CIPHER = AES_256_CBC;

    private final long BLOCK_SIZE = DEFAULT_CIPHER.getBlockSizeInBytes();

    public void size1024bCalculationWorksRoundTripAes128() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AES_128_CBC, size);
    }

    public void size1024bCalculationWorksRoundTripAes192() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AES_192_CBC, size);
    }

    public void size1024bCalculationWorksRoundTripAes256() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AES_256_CBC, size);
    }

    public void size0bCalculationWorksRoundTripAes128() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AES_128_CBC, size);
    }

    public void size0bCalculationWorksRoundTripAes192() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AES_192_CBC, size);
    }

    public void size0bCalculationWorksRoundTripAes256() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AES_256_CBC, size);
    }

    public void size2009125bCalculationWorksRoundTripAes128() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AES_128_CBC, size);
    }

    public void size2009125bCalculationWorksRoundTripAes192() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AES_192_CBC, size);
    }

    public void size2009125bCalculationWorksRoundTripAes256() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AES_256_CBC, size);
    }

    public void ciphertextSizeCalculationWorksForAes128() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AES_128_CBC);
    }

    public void ciphertextSizeCalculationWorksForAes192() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AES_192_CBC);
    }

    public void ciphertextSizeCalculationWorksForAes256() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AES_256_CBC);
    }

    public void calculateContentLengthWorks() {
        Assert.assertEquals(AesCbcCipherDetails.calculateContentLength(0, 16, 32, true), 48);
        Assert.assertEquals(AesCbcCipherDetails.calculateContentLength(1, 16, 32, true), 48);
        Assert.assertEquals(AesCbcCipherDetails.calculateContentLength(1, 16, 32, false), 48);
        Assert.assertEquals(AesCbcCipherDetails.calculateContentLength(16, 16, 32, false), 64);
        Assert.assertEquals(AesCbcCipherDetails.calculateContentLength(16, 16, 32, true), 48);
        Assert.assertEquals(AesCbcCipherDetails.calculateContentLength(17, 16, 32, true), 64);
        Assert.assertEquals(AesCbcCipherDetails.calculateContentLength(20, 16, 32, false), 64);
        Assert.assertEquals(AesCbcCipherDetails.calculateContentLength(32, 16, 32, true), 64);
        Assert.assertEquals(AesCbcCipherDetails.calculateContentLength(32, 16, 32, false), 80);
        Assert.assertEquals(AesCbcCipherDetails.calculateContentLength(33, 16, 32, false), 80);
        Assert.assertEquals(AesCbcCipherDetails.calculateContentLength(33, 16, 32, true), 80);
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

        testPlainToCipherStart(BLOCK_SIZE, 0, DEFAULT_CIPHER);
    }

    public void canTranslatePlainToCipherStartPositiveSecondBlockB() {

        testPlainToCipherStart((BLOCK_SIZE * 2) - 1, 0, DEFAULT_CIPHER);
    }

    public void canTranslatePlainToCipherStartPositiveThirdBlockA() {

        testPlainToCipherStart(BLOCK_SIZE * 2, BLOCK_SIZE, DEFAULT_CIPHER);
    }

    public void canTranslatePlainToCipherStartPositiveThirdBlockB() {

        testPlainToCipherStart((BLOCK_SIZE * 3) - 1, BLOCK_SIZE, DEFAULT_CIPHER);
    }

    public void canTranslatePlainToCipherStartNegativeFirstBlockA() {

        testPlainToCipherStart(-BLOCK_SIZE, -(BLOCK_SIZE * 2), DEFAULT_CIPHER);
    }

    public void canTranslatePlainToCipherStartNegativeFirstBlockB() {

        testPlainToCipherStart(-1, -(BLOCK_SIZE * 2), DEFAULT_CIPHER);
    }

    public void canTranslatePlainToCipherStartNegativeSecondBlockA() {

        testPlainToCipherStart(-(BLOCK_SIZE * 2), -(BLOCK_SIZE * 3), DEFAULT_CIPHER);
    }

    public void canTranslatePlainToCipherStartNegativeSecondBlockB() {

        testPlainToCipherStart(-(BLOCK_SIZE + 1), -(BLOCK_SIZE * 3), DEFAULT_CIPHER);
    }

    public void canTranslatePlainToCipherEndZero() {

        testPlainToCipherEnd(0, BLOCK_SIZE - 1, DEFAULT_CIPHER);
    }

    public void canTranslatePlainToCipherEndPositiveFirstBlockA() {

        testPlainToCipherEnd(1, BLOCK_SIZE - 1, DEFAULT_CIPHER);
    }

    public void canTranslatePlainToCipherEndPositiveFirstBlockB() {

        testPlainToCipherEnd(BLOCK_SIZE - 1, BLOCK_SIZE - 1, DEFAULT_CIPHER);
    }

    public void canTranslatePlainToCipherEndPositiveSecondBlockA() {

        testPlainToCipherEnd(BLOCK_SIZE, (BLOCK_SIZE * 2) - 1, DEFAULT_CIPHER);
    }

    public void canTranslatePlainToCipherEndPositiveSecondBlockB() {

        testPlainToCipherEnd((BLOCK_SIZE * 2) - 1, (BLOCK_SIZE * 2) - 1, DEFAULT_CIPHER);
    }

    public void canTranslatePlainToCipherEndNegativeFirstBlockA() {

        testPlainToCipherEnd(-1, -1, DEFAULT_CIPHER);
    }

    public void canTranslatePlainToCipherEndNegativeFirstBlockB() {

        testPlainToCipherEnd(-BLOCK_SIZE, -1, DEFAULT_CIPHER);
    }

    public void canTranslatePlainToCipherEndNegativeSecondBlockA() {

        testPlainToCipherEnd(-(BLOCK_SIZE + 1), -(BLOCK_SIZE + 1), DEFAULT_CIPHER);
    }

    public void canTranslatePlainToCipherEndNegativeSecondBlockB() {

        testPlainToCipherEnd(-(2 * BLOCK_SIZE), -(BLOCK_SIZE + 1), DEFAULT_CIPHER);
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

        testPlainToCipherOffset(-1, (BLOCK_SIZE * 2) - 1, DEFAULT_CIPHER);
    }

    public void canGetBlockCipherOffsetNonBlockBoundaryNegativeB() {

        testPlainToCipherOffset(-(BLOCK_SIZE - 1), BLOCK_SIZE + 1, DEFAULT_CIPHER);
    }

    public void canGetSecondBlockCipherOffsetBlockBoundaryPositive() {

        testPlainToCipherOffset(BLOCK_SIZE, BLOCK_SIZE, DEFAULT_CIPHER);
    }

    public void canGetSecondBlockCipherOffsetNonBlockBoundaryPositive() {

        testPlainToCipherOffset(BLOCK_SIZE + 1, BLOCK_SIZE + 1, DEFAULT_CIPHER);
    }

    public void canGetSecondBlockCipherOffsetBlockBoundaryNegative() {

        testPlainToCipherOffset(-(BLOCK_SIZE * 2), BLOCK_SIZE, DEFAULT_CIPHER);
    }

    public void canGetSecondBlockCipherOffsetNonBlockBoundaryNegative() {

        testPlainToCipherOffset(-((BLOCK_SIZE * 2) - 1), BLOCK_SIZE + 1, DEFAULT_CIPHER);
    }

}
