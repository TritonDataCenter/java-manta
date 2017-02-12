/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.crypto.SecretKey;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

@Test
public class SecretKeyUtilsTest {
    private final byte[] keyBytes;

    {
        keyBytes = "FFFFFFFBD96783C6C91E2222".getBytes(StandardCharsets.US_ASCII);
    }

    public void canGenerateAesGcmNoPaddingKey() {
        SecretKey key = SecretKeyUtils.generate(AesGcmCipherDetails.INSTANCE_128_BIT);
        Assert.assertNotNull(key, "Generated key was null");

        byte[] bytes = key.getEncoded();

        SecretKey loaded = SecretKeyUtils.loadKey(bytes, AesGcmCipherDetails.INSTANCE_128_BIT);

        Assert.assertEquals(loaded, key,
                "Generated key doesn't match loaded key");
    }

    public void canGenerateAesCtrNoPaddingKey() {
        SecretKey key = SecretKeyUtils.generate(AesCtrCipherDetails.INSTANCE_128_BIT);
        Assert.assertNotNull(key, "Generated key was null");

        byte[] bytes = key.getEncoded();

        SecretKey loaded = SecretKeyUtils.loadKey(bytes, AesCtrCipherDetails.INSTANCE_128_BIT);

        Assert.assertEquals(loaded, key,
                "Generated key doesn't match loaded key");
    }

    public void canGenerateAesCbcPkcs5PaddingKey() {
        SecretKey key = SecretKeyUtils.generate(AesCbcCipherDetails.INSTANCE_128_BIT);
        Assert.assertNotNull(key, "Generated key was null");

        byte[] bytes = key.getEncoded();

        SecretKey loaded = SecretKeyUtils.loadKey(bytes, AesCbcCipherDetails.INSTANCE_128_BIT);

        Assert.assertEquals(loaded, key,
                "Generated key doesn't match loaded key");
    }

    public void canLoadKeyFromURIPath() throws IOException {
        File file = File.createTempFile("ciphertext-", ".data");
        FileUtils.forceDeleteOnExit(file);
        FileUtils.writeByteArrayToFile(file, keyBytes);
        URI uri = URI.create("file://" + file.getAbsolutePath());

        SecretKey expected = SecretKeyUtils.loadKey(keyBytes, AesGcmCipherDetails.INSTANCE_128_BIT);
        SecretKey actual = SecretKeyUtils.loadKeyFromPath(Paths.get(uri), AesGcmCipherDetails.INSTANCE_128_BIT);

        Assert.assertEquals(actual.getAlgorithm(), expected.getAlgorithm());
        Assert.assertTrue(Arrays.equals(expected.getEncoded(), actual.getEncoded()),
                "Secret key loaded from URI doesn't match");
    }

    public void canLoadKeyFromFilePath() throws IOException {
        File file = File.createTempFile("ciphertext-", ".data");
        FileUtils.forceDeleteOnExit(file);
        FileUtils.writeByteArrayToFile(file, keyBytes);
        Path path = file.toPath();

        SecretKey expected = SecretKeyUtils.loadKey(keyBytes, AesGcmCipherDetails.INSTANCE_128_BIT);
        SecretKey actual = SecretKeyUtils.loadKeyFromPath(path, AesGcmCipherDetails.INSTANCE_128_BIT);

        Assert.assertEquals(actual.getAlgorithm(), expected.getAlgorithm());
        Assert.assertTrue(Arrays.equals(expected.getEncoded(), actual.getEncoded()),
                "Secret key loaded from URI doesn't match");
    }

    @Test(expectedExceptions = Exception.class)
    public void writeKeyWithNullKey() throws IOException {
        SecretKeyUtils.writeKey(null, null);
    }

    @Test(expectedExceptions = Exception.class)
    public void writeKeyWithNullOutStream() throws IOException {
        SupportedCipherDetails cipherDetails = AesGcmCipherDetails.INSTANCE_128_BIT;
        byte[] keyBytes = SecretKeyUtils.generate(cipherDetails).getEncoded();

        SecretKey key = SecretKeyUtils.loadKey(Arrays.copyOfRange(keyBytes, 2, 10), cipherDetails);
        SecretKeyUtils.writeKey(key, null);
    }

    @Test(expectedExceptions = Exception.class)
    public void writeKeyToPathWithNullKey() throws IOException {
        SecretKeyUtils.writeKeyToPath(null, null);
    }

    public void writeKeyToPath() throws IOException {
        File file = File.createTempFile("ciphertext-", ".data");
        FileUtils.forceDeleteOnExit(file);
        Path path = file.toPath();

        SupportedCipherDetails cipherDetails = AesGcmCipherDetails.INSTANCE_128_BIT;
        byte[] keyBytes = SecretKeyUtils.generate(cipherDetails).getEncoded();
        SecretKey key = SecretKeyUtils.loadKey(keyBytes, cipherDetails);
        SecretKeyUtils.writeKeyToPath(key, path);

        SecretKey actual = SecretKeyUtils.loadKeyFromPath(path, cipherDetails);
        Assert.assertEquals(actual.getAlgorithm(), key.getAlgorithm());
        Assert.assertTrue(Arrays.equals(key.getEncoded(), actual.getEncoded()));
    }
}
