package com.joyent.manta.client.crypto;

import org.testng.annotations.Test;

@Test
public class AesGcmCipherDetailsTest extends AbstractCipherDetailsTest {
    public void size1024bCalculationWorksRoundTripAes128() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AesGcmCipherDetails.INSTANCE_128_BIT, size);
    }

    public void size1024bCalculationWorksRoundTripAes192() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AesGcmCipherDetails.INSTANCE_192_BIT, size);
    }

    public void size1024bCalculationWorksRoundTripAes256() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AesGcmCipherDetails.INSTANCE_256_BIT, size);
    }

    public void size0bCalculationWorksRoundTripAes128() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AesGcmCipherDetails.INSTANCE_128_BIT, size);
    }

    public void size0bCalculationWorksRoundTripAes192() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AesGcmCipherDetails.INSTANCE_192_BIT, size);
    }

    public void size0bCalculationWorksRoundTripAes256() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AesGcmCipherDetails.INSTANCE_256_BIT, size);
    }

    public void size2009125bCalculationWorksRoundTripAes128() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AesGcmCipherDetails.INSTANCE_128_BIT, size);
    }

    public void size2009125bCalculationWorksRoundTripAes192() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AesGcmCipherDetails.INSTANCE_192_BIT, size);
    }

    public void size2009125bCalculationWorksRoundTripAes256() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AesGcmCipherDetails.INSTANCE_256_BIT, size);
    }

    public void ciphertextSizeCalculationWorksForAes128() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AesGcmCipherDetails.INSTANCE_128_BIT);
    }

    public void ciphertextSizeCalculationWorksForAes192() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AesGcmCipherDetails.INSTANCE_192_BIT);
    }

    public void ciphertextSizeCalculationWorksForAes256() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AesGcmCipherDetails.INSTANCE_256_BIT);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void canQueryCiphertextByteRangeAes128() throws Exception {
        SupportedCipherDetails cipherDetails = AesGcmCipherDetails.INSTANCE_128_BIT;
        cipherDetails.translateByteRange(0, 128);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void canQueryCiphertextByteRangeAes192() throws Exception {
        SupportedCipherDetails cipherDetails = AesGcmCipherDetails.INSTANCE_192_BIT;
        cipherDetails.translateByteRange(0, 128);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void canQueryCiphertextByteRangeAes256() throws Exception {
        SupportedCipherDetails cipherDetails = AesGcmCipherDetails.INSTANCE_256_BIT;
        cipherDetails.translateByteRange(0, 128);
    }
}
