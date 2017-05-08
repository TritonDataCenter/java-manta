/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import java.security.NoSuchAlgorithmException;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test
public class AesGcmCipherDetailsTest extends AbstractCipherDetailsTest {

    private AesGcmCipherDetails AES_GCM_128;

    private AesGcmCipherDetails AES_GCM_192;

    private AesGcmCipherDetails AES_GCM_256;

    @BeforeClass
    private void init() throws NoSuchAlgorithmException {

        AES_GCM_128 = AesGcmCipherDetails.aesGcm128();
        AES_GCM_192 = AesGcmCipherDetails.aesGcm192();
        AES_GCM_256 = AesGcmCipherDetails.aesGcm256();
    }

    public void doesntCalculateHmac() throws Exception {
        Assert.assertEquals(AES_GCM_256.getAuthenticationHmac(), null);
    }

    public void size1024bCalculationWorksRoundTripAes128() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AES_GCM_128, size);
    }

    public void size1024bCalculationWorksRoundTripAes192() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AES_GCM_192, size);
    }

    public void size1024bCalculationWorksRoundTripAes256() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AES_GCM_256, size);
    }

    public void size0bCalculationWorksRoundTripAes128() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AES_GCM_128, size);
    }

    public void size0bCalculationWorksRoundTripAes192() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AES_GCM_192, size);
    }

    public void size0bCalculationWorksRoundTripAes256() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AES_GCM_256, size);
    }

    public void size2009125bCalculationWorksRoundTripAes128() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AES_GCM_128, size);
    }

    public void size2009125bCalculationWorksRoundTripAes192() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AES_GCM_192, size);
    }

    public void size2009125bCalculationWorksRoundTripAes256() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AES_GCM_256, size);
    }

    public void ciphertextSizeCalculationWorksForAes128() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AES_GCM_128);
    }

    public void ciphertextSizeCalculationWorksForAes192() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AES_GCM_192);
    }

    public void ciphertextSizeCalculationWorksForAes256() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AES_GCM_256);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void canQueryCiphertextByteRangeAes128() throws Exception {
        SupportedCipherDetails cipherDetails = AES_GCM_128;
        cipherDetails.translateByteRange(0, 128);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void canQueryCiphertextByteRangeAes192() throws Exception {
        SupportedCipherDetails cipherDetails = AES_GCM_192;
        cipherDetails.translateByteRange(0, 128);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void canQueryCiphertextByteRangeAes256() throws Exception {
        SupportedCipherDetails cipherDetails = AES_GCM_256;
        cipherDetails.translateByteRange(0, 128);
    }
}
