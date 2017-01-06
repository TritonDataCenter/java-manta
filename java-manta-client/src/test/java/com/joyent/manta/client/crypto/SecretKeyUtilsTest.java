package com.joyent.manta.client.crypto;

import org.testng.Assert;
import org.testng.annotations.Test;

import javax.crypto.SecretKey;

@Test
public class SecretKeyUtilsTest {
    private final SupportedCipherDetails aesGcmCipherDetails = AesGcmCipherDetails.INSTANCE;

    public void canGenerateAesGCMNoPaddingKey() {
        SecretKey key = SecretKeyUtils.generate(aesGcmCipherDetails);
        Assert.assertNotNull(key, "Generated key was null");
    }
}
