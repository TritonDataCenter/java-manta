package com.joyent.manta.client.crypto;

import org.testng.Assert;
import org.testng.annotations.Test;

import javax.crypto.SecretKey;

@Test
public class SecretKeyUtilsTest {


    public void canGenerateAesGcmNoPaddingKey() {
        SecretKey key = SecretKeyUtils.generate(AesGcmCipherDetails.INSTANCE);
        Assert.assertNotNull(key, "Generated key was null");

        byte[] bytes = key.getEncoded();

        SecretKey loaded = SecretKeyUtils.loadKey(bytes, AesGcmCipherDetails.INSTANCE);

        Assert.assertEquals(loaded, key,
                "Generated key doesn't match loaded key");
    }

    public void canGenerateAesCtrNoPaddingKey() {
        SecretKey key = SecretKeyUtils.generate(AesCtrCipherDetails.INSTANCE);
        Assert.assertNotNull(key, "Generated key was null");

        byte[] bytes = key.getEncoded();

        SecretKey loaded = SecretKeyUtils.loadKey(bytes, AesCtrCipherDetails.INSTANCE);

        Assert.assertEquals(loaded, key,
                "Generated key doesn't match loaded key");
    }

    public void canGenerateAesCbcPkcs5PaddingKey() {
        SecretKey key = SecretKeyUtils.generate(AesCbcCipherDetails.INSTANCE);
        Assert.assertNotNull(key, "Generated key was null");

        byte[] bytes = key.getEncoded();

        SecretKey loaded = SecretKeyUtils.loadKey(bytes, AesCbcCipherDetails.INSTANCE);

        Assert.assertEquals(loaded, key,
                "Generated key doesn't match loaded key");
    }
}
