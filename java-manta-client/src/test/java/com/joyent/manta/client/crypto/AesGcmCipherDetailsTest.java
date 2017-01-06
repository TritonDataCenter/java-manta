package com.joyent.manta.client.crypto;

import org.testng.Assert;
import org.testng.annotations.Test;

import javax.crypto.Cipher;

@Test
public class AesGcmCipherDetailsTest {
    private final SupportedCipherDetails aesGcmCipherDetails = AesGcmCipherDetails.INSTANCE;

    public void canLoadCipher() {
        Cipher cipher = aesGcmCipherDetails.getCipher();
        Assert.assertNotNull(cipher, "Cipher returned was null");
    }
}
