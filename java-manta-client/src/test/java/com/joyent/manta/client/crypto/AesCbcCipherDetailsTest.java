/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import org.testng.Assert;
import org.testng.annotations.Test;

import javax.crypto.SecretKey;

@Test
public class AesCbcCipherDetailsTest extends AbstractCipherDetailsTest {
    public void size1024bCalculationWorksRoundTripAes128() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AesCbcCipherDetails.INSTANCE_128_BIT, size);
    }

    @Test(groups = {"unlimited-crypto"})
    public void size1024bCalculationWorksRoundTripAes192() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AesCbcCipherDetails.INSTANCE_192_BIT, size);
    }

    @Test(groups = {"unlimited-crypto"})
    public void size1024bCalculationWorksRoundTripAes256() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AesCbcCipherDetails.INSTANCE_256_BIT, size);
    }

    public void size0bCalculationWorksRoundTripAes128() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AesCbcCipherDetails.INSTANCE_128_BIT, size);
    }

    @Test(groups = {"unlimited-crypto"})
    public void size0bCalculationWorksRoundTripAes192() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AesCbcCipherDetails.INSTANCE_192_BIT, size);
    }

    @Test(groups = {"unlimited-crypto"})
    public void size0bCalculationWorksRoundTripAes256() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AesCbcCipherDetails.INSTANCE_256_BIT, size);
    }

    public void size2009125bCalculationWorksRoundTripAes128() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AesCbcCipherDetails.INSTANCE_128_BIT, size);
    }

    @Test(groups = {"unlimited-crypto"})
    public void size2009125bCalculationWorksRoundTripAes192() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AesCbcCipherDetails.INSTANCE_192_BIT, size);
    }

    @Test(groups = {"unlimited-crypto"})
    public void size2009125bCalculationWorksRoundTripAes256() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AesCbcCipherDetails.INSTANCE_256_BIT, size);
    }

    public void ciphertextSizeCalculationWorksForAes128() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AesCbcCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void ciphertextSizeCalculationWorksForAes192() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AesCbcCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void ciphertextSizeCalculationWorksForAes256() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AesCbcCipherDetails.INSTANCE_256_BIT);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void canQueryCiphertextByteRangeAes128() throws Exception {
        SupportedCipherDetails cipherDetails = AesCbcCipherDetails.INSTANCE_128_BIT;
        SecretKey secretKey = SecretKeyUtils.generate(cipherDetails);
        cipherDetails.translateByteRange(0, 128);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class, groups = {"unlimited-crypto"})
    public void canQueryCiphertextByteRangeAes192() throws Exception {
        SupportedCipherDetails cipherDetails = AesCbcCipherDetails.INSTANCE_192_BIT;
        SecretKey secretKey = SecretKeyUtils.generate(cipherDetails);
        cipherDetails.translateByteRange(0, 128);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class, groups = {"unlimited-crypto"})
    public void canQueryCiphertextByteRangeAes256() throws Exception {
        SupportedCipherDetails cipherDetails = AesCbcCipherDetails.INSTANCE_256_BIT;
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
