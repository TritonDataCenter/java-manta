package com.joyent.manta.client.crypto;

import org.testng.Assert;
import org.testng.annotations.Test;

import javax.crypto.Cipher;

@Test
public class SupportedCipherDetailsTest {
    public void canGetAllCiphers() {
        SupportedCipherDetails aesGcm =
                SupportedCipherDetails.SUPPORTED_CIPHERS.get("AES/GCM/NoPadding");
        Assert.assertTrue(aesGcm instanceof AesGcmCipherDetails);

        SupportedCipherDetails aesCtr =
                SupportedCipherDetails.SUPPORTED_CIPHERS.get("AES/CTR/NoPadding");
        Assert.assertTrue(aesCtr instanceof AesCtrCipherDetails);

        SupportedCipherDetails aesCbc =
                SupportedCipherDetails.SUPPORTED_CIPHERS.get("AES/CBC/PKCS5Padding");
        Assert.assertTrue(aesCbc instanceof AesCbcCipherDetails);
    }

    public void canFindCiphers() {
        for (String algorithm : SupportedCipherDetails.SUPPORTED_CIPHERS.keySet()) {
            Cipher cipher = SupportedCipherDetails.findCipher(algorithm,
                    BouncyCastleLoader.BOUNCY_CASTLE_PROVIDER);
            Assert.assertNotNull(cipher, "Couldn't find cipher for algorithm: " + algorithm);

            Cipher cipherLowercase = SupportedCipherDetails.findCipher(algorithm.toLowerCase(),
                    BouncyCastleLoader.BOUNCY_CASTLE_PROVIDER);
            Assert.assertNotNull(cipherLowercase, "Couldn't find cipher for algorithm: " + algorithm);
        }
    }
}
