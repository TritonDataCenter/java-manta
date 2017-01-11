package com.joyent.manta.client.crypto;

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.crypto.SecretKey;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

@Test
public class SecretKeyUtilsTest {
    private final byte[] keyBytes;

    {
        try {
            keyBytes = "FFFFFFFBD96783C6C91E2222".getBytes("US-ASCII");
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError(e.getMessage());
        }
    }

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

    public void canLoadKeyFromURIPath() throws IOException {
        File file = File.createTempFile("ciphertext-", ".data");
        FileUtils.forceDeleteOnExit(file);
        FileUtils.writeByteArrayToFile(file, keyBytes);
        URI uri = URI.create("file://" + file.getAbsolutePath());

        SecretKey expected = SecretKeyUtils.loadKey(keyBytes, AesGcmCipherDetails.INSTANCE);
        SecretKey actual = SecretKeyUtils.loadKeyFromPath(Paths.get(uri), AesGcmCipherDetails.INSTANCE);

        Assert.assertEquals(actual.getAlgorithm(), expected.getAlgorithm());
        Assert.assertTrue(Arrays.equals(expected.getEncoded(), actual.getEncoded()),
                "Secret key loaded from URI doesn't match");
    }

    public void canLoadKeyFromFilePath() throws IOException {
        File file = File.createTempFile("ciphertext-", ".data");
        FileUtils.forceDeleteOnExit(file);
        FileUtils.writeByteArrayToFile(file, keyBytes);
        Path path = file.toPath();

        SecretKey expected = SecretKeyUtils.loadKey(keyBytes, AesGcmCipherDetails.INSTANCE);
        SecretKey actual = SecretKeyUtils.loadKeyFromPath(path, AesGcmCipherDetails.INSTANCE);

        Assert.assertEquals(actual.getAlgorithm(), expected.getAlgorithm());
        Assert.assertTrue(Arrays.equals(expected.getEncoded(), actual.getEncoded()),
                "Secret key loaded from URI doesn't match");
    }
}
