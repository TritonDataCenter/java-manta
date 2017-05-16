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

@Test
public class AesGcmCipherDetailsTest extends AbstractCipherDetailsTest {

    @Test(groups = {"unlimited-crypto"})
    public void doesntCalculateHmac() throws Exception {
        Assert.assertEquals(AesGcmCipherDetails.INSTANCE_256_BIT.getAuthenticationHmac(), null);
    }

    public void size1024bCalculationWorksRoundTripAes128() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AesGcmCipherDetails.INSTANCE_128_BIT, size);
    }

    @Test(groups = {"unlimited-crypto"})
    public void size1024bCalculationWorksRoundTripAes192() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AesGcmCipherDetails.INSTANCE_192_BIT, size);
    }

    @Test(groups = {"unlimited-crypto"})
    public void size1024bCalculationWorksRoundTripAes256() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AesGcmCipherDetails.INSTANCE_256_BIT, size);
    }

    public void size0bCalculationWorksRoundTripAes128() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AesGcmCipherDetails.INSTANCE_128_BIT, size);
    }

    @Test(groups = {"unlimited-crypto"})
    public void size0bCalculationWorksRoundTripAes192() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AesGcmCipherDetails.INSTANCE_192_BIT, size);
    }

    @Test(groups = {"unlimited-crypto"})
    public void size0bCalculationWorksRoundTripAes256() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AesGcmCipherDetails.INSTANCE_256_BIT, size);
    }

    public void size2009125bCalculationWorksRoundTripAes128() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AesGcmCipherDetails.INSTANCE_128_BIT, size);
    }

    @Test(groups = {"unlimited-crypto"})
    public void size2009125bCalculationWorksRoundTripAes192() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AesGcmCipherDetails.INSTANCE_192_BIT, size);
    }

    @Test(groups = {"unlimited-crypto"})
    public void size2009125bCalculationWorksRoundTripAes256() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AesGcmCipherDetails.INSTANCE_256_BIT, size);
    }

    public void ciphertextSizeCalculationWorksForAes128() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AesGcmCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void ciphertextSizeCalculationWorksForAes192() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AesGcmCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void ciphertextSizeCalculationWorksForAes256() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AesGcmCipherDetails.INSTANCE_256_BIT);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void canQueryCiphertextByteRangeAes128() throws Exception {
        SupportedCipherDetails cipherDetails = AesGcmCipherDetails.INSTANCE_128_BIT;
        cipherDetails.translateByteRange(0, 128);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class, groups = {"unlimited-crypto"})
    public void canQueryCiphertextByteRangeAes192() throws Exception {
        SupportedCipherDetails cipherDetails = AesGcmCipherDetails.INSTANCE_192_BIT;
        cipherDetails.translateByteRange(0, 128);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class, groups = {"unlimited-crypto"})
    public void canQueryCiphertextByteRangeAes256() throws Exception {
        SupportedCipherDetails cipherDetails = AesGcmCipherDetails.INSTANCE_256_BIT;
        cipherDetails.translateByteRange(0, 128);
    }
}
