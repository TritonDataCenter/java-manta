/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import com.joyent.manta.exception.MantaClientEncryptionException;
import com.joyent.manta.http.entity.ExposedStringEntity;
import com.joyent.manta.http.entity.MantaInputStreamEntity;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.http.HttpEntity;
import org.bouncycastle.jcajce.io.CipherInputStream;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.function.Predicate;

@Test
public class EncryptingEntityTest {

    private AesCbcCipherDetails AES_CBC_128;

    private AesCtrCipherDetails AES_CTR_128;

    private AesGcmCipherDetails AES_GCM_128;

    @BeforeClass
    private void init() throws NoSuchAlgorithmException {

        AES_CBC_128 = AesCbcCipherDetails.aesCbc128();
        AES_CTR_128 = AesCtrCipherDetails.aesCtr128();
        AES_GCM_128 = AesGcmCipherDetails.aesGcm128();
    }
    /* Constructor Tests */

    @Test(expectedExceptions = MantaClientEncryptionException.class)
    public void throwsWithTooLargeContentLength() {
        SupportedCipherDetails cipherDetails = AES_GCM_128;
        byte[] keyBytes = SecretKeyUtils.generate(cipherDetails).getEncoded();

        SecretKey key = SecretKeyUtils.loadKey(Arrays.copyOfRange(keyBytes, 2, 10), cipherDetails);

        ExposedStringEntity stringEntity = new ExposedStringEntity("boo",
                StandardCharsets.US_ASCII);

        EncryptingEntity entity = new EncryptingEntity(key, cipherDetails, stringEntity);
    }

    /* AES-GCM-NoPadding Tests */

    public void canEncryptAndDecryptToAndFromFileInAesGcm() throws Exception {
        verifyEncryptionWorksRoundTrip(AES_GCM_128);
    }

    public void canEncryptAndDecryptToAndFromFileWithManySizesInAesGcm() throws Exception {
        canEncryptAndDecryptToAndFromFileWithManySizes(AES_GCM_128);
    }

    public void canCountBytesFromStreamWithUnknownLengthInAesGcm() throws Exception {
        canCountBytesFromStreamWithUnknownLength(AES_GCM_128);
    }

    /* AES-CTR-NoPadding Tests */

    public void canEncryptAndDecryptToAndFromFileInAesCtr() throws Exception {
        verifyEncryptionWorksRoundTrip(AES_CTR_128);
    }

    public void canEncryptAndDecryptToAndFromFileWithManySizesInAesCtr() throws Exception {
        canEncryptAndDecryptToAndFromFileWithManySizes(AES_CTR_128);
    }

    public void canCountBytesFromStreamWithUnknownLengthInAesCtr() throws Exception {
        canCountBytesFromStreamWithUnknownLength(AES_CTR_128);
    }

    /* AES-CBC-PKCS5Padding Tests */

    public void canEncryptAndDecryptToAndFromFileInAesCbc() throws Exception {
        verifyEncryptionWorksRoundTrip(AES_CBC_128);
    }

    public void canEncryptAndDecryptToAndFromFileWithManySizesInAesCbc() throws Exception {
        canEncryptAndDecryptToAndFromFileWithManySizes(AES_CBC_128);
    }

    public void canCountBytesFromStreamWithUnknownLengthInAesCbc() throws Exception {
        canCountBytesFromStreamWithUnknownLength(AES_CBC_128);
    }

    /* Test helper methods */

    private void canCountBytesFromStreamWithUnknownLength(SupportedCipherDetails cipherDetails)
            throws Exception {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URL resource = classLoader.getResource("com/joyent/manta/client/crypto/EncryptingEntityTest.class");
        Path path = Paths.get(resource.toURI());
        long size = path.toFile().length();

        MantaInputStreamEntity entity = new MantaInputStreamEntity(resource.openStream());

        Assert.assertEquals(entity.getContentLength(), -1L,
                "Content length should be set to unknown value");

        byte[] keyBytes = SecretKeyUtils.generate(cipherDetails).getEncoded();

        verifyEncryptionWorksRoundTrip(keyBytes, cipherDetails,
                entity, (actualBytes) -> {
                    Assert.assertEquals(actualBytes.length, size,
                            "Incorrect number of bytes counted");
                    return true;
                });
    }

    private void canEncryptAndDecryptToAndFromFileWithManySizes(SupportedCipherDetails cipherDetails)
            throws Exception {
        final Charset charset = StandardCharsets.US_ASCII;
        final int maxLength = 1025;

        for (int i = 0; i < maxLength; i++) {
            final char[] chars = new char[i];
            Arrays.fill(chars, 'z');
            final String expectedString = String.valueOf(chars);

            ExposedStringEntity stringEntity = new ExposedStringEntity(
                    expectedString, charset);

            byte[] keyBytes = SecretKeyUtils.generate(cipherDetails).getEncoded();

            verifyEncryptionWorksRoundTrip(keyBytes, cipherDetails,
                    stringEntity, (actualBytes) -> {
                        final String actual = new String(actualBytes, charset);
                        Assert.assertEquals(actual, expectedString,
                                "Plaintext doesn't match decrypted value");
                        return true;
                    });
        }
    }

    private static void verifyEncryptionWorksRoundTrip(SupportedCipherDetails cipherDetails) throws Exception {
        byte[] keyBytes = SecretKeyUtils.generate(cipherDetails).getEncoded();
        final Charset charset = StandardCharsets.US_ASCII;
        final String expectedString = "012345678901245601234567890124";
        ExposedStringEntity stringEntity = new ExposedStringEntity(
                expectedString, charset);

        verifyEncryptionWorksRoundTrip(keyBytes, cipherDetails, stringEntity,
                (actualBytes) -> {
            final String actual = new String(actualBytes, charset);
            Assert.assertEquals(actual, expectedString,
                    "Plaintext doesn't match decrypted value");
            return true;
        });
    }

    private static void verifyEncryptionWorksRoundTrip(byte[] keyBytes,
                                                       SupportedCipherDetails cipherDetails,
                                                       HttpEntity entity,
                                                       Predicate<byte[]> validator)
            throws Exception {
        SecretKey key = SecretKeyUtils.loadKey(keyBytes, cipherDetails);

        EncryptingEntity encryptingEntity = new EncryptingEntity(key,
                cipherDetails, entity);

        File file = File.createTempFile("ciphertext-", ".data");
        FileUtils.forceDeleteOnExit(file);

        try (FileOutputStream out = new FileOutputStream(file)) {
            encryptingEntity.writeTo(out);
        }

        Assert.assertEquals(file.length(), encryptingEntity.getContentLength(),
                "Expected ciphertext file size doesn't match actual file size " +
                        "[originalContentLength=" + entity.getContentLength() + "] -");

        byte[] iv = encryptingEntity.getCipher().getIV();
        Cipher cipher = cipherDetails.getCipher();
        cipher.init(Cipher.DECRYPT_MODE, key, cipherDetails.getEncryptionParameterSpec(iv));

        final long ciphertextSize;

        if (cipherDetails.isAEADCipher()) {
            ciphertextSize = encryptingEntity.getContentLength();
        } else {
            ciphertextSize = encryptingEntity.getContentLength() - cipherDetails.getAuthenticationTagOrHmacLengthInBytes();
        }

        try (FileInputStream in = new FileInputStream(file);
             BoundedInputStream bin = new BoundedInputStream(in, ciphertextSize);
             CipherInputStream cin = new CipherInputStream(bin, cipher)) {
            final byte[] actualBytes = IOUtils.toByteArray(cin);

            final byte[] hmacBytes = new byte[cipherDetails.getAuthenticationTagOrHmacLengthInBytes()];
            in.read(hmacBytes);

            Assert.assertTrue(validator.test(actualBytes),
                    "Entity validation failed");

        }
    }
}
