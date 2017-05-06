/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.security.NoSuchAlgorithmException;

import javax.crypto.SecretKey;

@Test
public class AesCbcCipherDetailsTest extends AbstractCipherDetailsTest {

    private AesCbcCipherDetails AES_CBC_128;

    private AesCbcCipherDetails AES_CBC_192;

    private AesCbcCipherDetails AES_CBC_256;

    @BeforeClass
    private void init() throws NoSuchAlgorithmException {

        AES_CBC_128 = AesCbcCipherDetails.aesCbc128();
        AES_CBC_192 = AesCbcCipherDetails.aesCbc192();
        AES_CBC_256 = AesCbcCipherDetails.aesCbc256();
    }

    public void size1024bCalculationWorksRoundTripAes128() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AES_CBC_128, size);
    }

    public void size1024bCalculationWorksRoundTripAes192() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AES_CBC_192, size);
    }

    public void size1024bCalculationWorksRoundTripAes256() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AES_CBC_256, size);
    }

    public void size0bCalculationWorksRoundTripAes128() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AES_CBC_128, size);
    }

    public void size0bCalculationWorksRoundTripAes192() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AES_CBC_192, size);
    }

    public void size0bCalculationWorksRoundTripAes256() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AES_CBC_256, size);
    }

    public void size2009125bCalculationWorksRoundTripAes128() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AES_CBC_128, size);
    }

    public void size2009125bCalculationWorksRoundTripAes192() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AES_CBC_192, size);
    }

    public void size2009125bCalculationWorksRoundTripAes256() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AES_CBC_256, size);
    }

    public void ciphertextSizeCalculationWorksForAes128() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AES_CBC_128);
    }

    public void ciphertextSizeCalculationWorksForAes192() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AES_CBC_192);
    }

    public void ciphertextSizeCalculationWorksForAes256() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AES_CBC_256);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void canQueryCiphertextByteRangeAes128() throws Exception {
        SupportedCipherDetails cipherDetails = AES_CBC_128;
        SecretKey secretKey = SecretKeyUtils.generate(cipherDetails);
        cipherDetails.translateByteRange(0, 128);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void canQueryCiphertextByteRangeAes192() throws Exception {
        SupportedCipherDetails cipherDetails = AES_CBC_192;
        SecretKey secretKey = SecretKeyUtils.generate(cipherDetails);
        cipherDetails.translateByteRange(0, 128);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void canQueryCiphertextByteRangeAes256() throws Exception {
        SupportedCipherDetails cipherDetails = AES_CBC_256;
        SecretKey secretKey = SecretKeyUtils.generate(cipherDetails);
        cipherDetails.translateByteRange(0, 128);
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
}
