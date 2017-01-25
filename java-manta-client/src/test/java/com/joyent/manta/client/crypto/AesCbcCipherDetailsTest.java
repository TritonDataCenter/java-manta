package com.joyent.manta.client.crypto;

import org.testng.annotations.Test;

import javax.crypto.SecretKey;

@Test
public class AesCbcCipherDetailsTest extends AbstractCipherDetailsTest {
    public void size1024bCalculationWorksRoundTripAes128() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AesCbcCipherDetails.INSTANCE_128_BIT, size);
    }

    public void size1024bCalculationWorksRoundTripAes192() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AesCbcCipherDetails.INSTANCE_192_BIT, size);
    }

    public void size1024bCalculationWorksRoundTripAes256() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AesCbcCipherDetails.INSTANCE_256_BIT, size);
    }

    public void size0bCalculationWorksRoundTripAes128() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AesCbcCipherDetails.INSTANCE_128_BIT, size);
    }

    public void size0bCalculationWorksRoundTripAes192() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AesCbcCipherDetails.INSTANCE_192_BIT, size);
    }

    public void size0bCalculationWorksRoundTripAes256() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AesCbcCipherDetails.INSTANCE_256_BIT, size);
    }

    public void size2009125bCalculationWorksRoundTripAes128() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AesCbcCipherDetails.INSTANCE_128_BIT, size);
    }

    public void size2009125bCalculationWorksRoundTripAes192() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AesCbcCipherDetails.INSTANCE_192_BIT, size);
    }

    public void size2009125bCalculationWorksRoundTripAes256() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AesCbcCipherDetails.INSTANCE_256_BIT, size);
    }

    public void ciphertextSizeCalculationWorksForAes128() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AesCbcCipherDetails.INSTANCE_128_BIT);
    }

    public void ciphertextSizeCalculationWorksForAes192() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AesCbcCipherDetails.INSTANCE_192_BIT);
    }

    public void ciphertextSizeCalculationWorksForAes256() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AesCbcCipherDetails.INSTANCE_256_BIT);
    }

    public void canQueryCiphertextByteRangeAes128() throws Exception {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
        SecretKey secretKey = SecretKeyUtils.generate(cipherDetails);
        canRandomlyReadPlaintextPositionFromCiphertext(secretKey, cipherDetails);
    }

    public void canQueryCiphertextByteRangeAes192() throws Exception {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_192_BIT;
        SecretKey secretKey = SecretKeyUtils.generate(cipherDetails);
        canRandomlyReadPlaintextPositionFromCiphertext(secretKey, cipherDetails);
    }

    public void canQueryCiphertextByteRangeAes256() throws Exception {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_256_BIT;
        SecretKey secretKey = SecretKeyUtils.generate(cipherDetails);
        canRandomlyReadPlaintextPositionFromCiphertext(secretKey, cipherDetails);
    }
}
