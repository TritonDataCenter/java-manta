/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.exception.MantaClientEncryptionException;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.http.entity.ExposedStringEntity;
import com.joyent.manta.http.entity.MantaInputStreamEntity;
import com.joyent.manta.util.FailingOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.conn.EofSensorInputStream;
import org.bouncycastle.jcajce.io.CipherInputStream;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;
import java.util.function.Predicate;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;

@Test
public class EncryptingEntityTest {
    /* Constructor Tests */

    @Test(expectedExceptions = MantaClientEncryptionException.class)
    public void throwsWithTooLargeContentLength() {
        SupportedCipherDetails cipherDetails = AesGcmCipherDetails.INSTANCE_128_BIT;
        byte[] keyBytes = SecretKeyUtils.generate(cipherDetails).getEncoded();

        SecretKey key = SecretKeyUtils.loadKey(Arrays.copyOfRange(keyBytes, 2, 10), cipherDetails);

        ExposedStringEntity stringEntity = new ExposedStringEntity("boo",
                StandardCharsets.US_ASCII);

        EncryptingEntity entity = new EncryptingEntity(key, cipherDetails, stringEntity);
    }

    /* AES-GCM-NoPadding Tests */

    public void canEncryptAndDecryptToAndFromFileInAesGcm() throws Exception {
        verifyEncryptionWorksRoundTrip(AesGcmCipherDetails.INSTANCE_128_BIT);
    }

    public void canEncryptAndDecryptToAndFromFileWithManySizesInAesGcm() throws Exception {
        canEncryptAndDecryptToAndFromFileWithManySizes(AesGcmCipherDetails.INSTANCE_128_BIT);
    }

    public void canCountBytesFromStreamWithUnknownLengthInAesGcm() throws Exception {
        canCountBytesFromStreamWithUnknownLength(AesGcmCipherDetails.INSTANCE_128_BIT);
    }

    public void canSurviveNetworkFailuresInAesGcm() throws Exception {
        canBeWrittenIdempotently(AesGcmCipherDetails.INSTANCE_128_BIT);
    }

    /* AES-CTR-NoPadding Tests */

    public void canEncryptAndDecryptToAndFromFileInAesCtr() throws Exception {
        verifyEncryptionWorksRoundTrip(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    public void canEncryptAndDecryptToAndFromFileWithManySizesInAesCtr() throws Exception {
        canEncryptAndDecryptToAndFromFileWithManySizes(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    public void canCountBytesFromStreamWithUnknownLengthInAesCtr() throws Exception {
        canCountBytesFromStreamWithUnknownLength(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    public void canSurviveNetworkFailuresInAesCtr() throws Exception {
        canBeWrittenIdempotently(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    /* AES-CBC-PKCS5Padding Tests */

    public void canEncryptAndDecryptToAndFromFileInAesCbc() throws Exception {
        verifyEncryptionWorksRoundTrip(AesCbcCipherDetails.INSTANCE_128_BIT);
    }

    public void canEncryptAndDecryptToAndFromFileWithManySizesInAesCbc() throws Exception {
        canEncryptAndDecryptToAndFromFileWithManySizes(AesCbcCipherDetails.INSTANCE_128_BIT);
    }

    public void canCountBytesFromStreamWithUnknownLengthInAesCbc() throws Exception {
        canCountBytesFromStreamWithUnknownLength(AesCbcCipherDetails.INSTANCE_128_BIT);
    }

    public void canSurviveNetworkFailuresInAesCbc() throws Exception {
        canBeWrittenIdempotently(AesCbcCipherDetails.INSTANCE_128_BIT);
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

    private void canBeWrittenIdempotently(final SupportedCipherDetails cipherDetails) throws Exception {
        final SecretKey secretKey = SecretKeyUtils.generate(cipherDetails);
        final String content = RandomStringUtils.randomAlphanumeric(RandomUtils.nextInt(500, 1500));
        final ExposedStringEntity contentEntity = new ExposedStringEntity(
                content,
                StandardCharsets.UTF_8);

        final ByteArrayOutputStream referenceEncrypted = new ByteArrayOutputStream(content.length());
        {

            final EncryptingEntity referenceEntity = new EncryptingEntity(
                    secretKey,
                    cipherDetails,
                    contentEntity);

            referenceEntity.writeTo(referenceEncrypted);
            validateCiphertext(
                    cipherDetails,
                    secretKey,
                    contentEntity.getBackingBuffer().array(),
                    referenceEntity.getCipher().getIV(),
                    referenceEncrypted.toByteArray());
        }

        final ByteArrayOutputStream retryEncrypted = new ByteArrayOutputStream(content.length());
        final FailingOutputStream output = new FailingOutputStream(retryEncrypted, content.length() / 2);
        {
            final EncryptingEntity retryingEntity = new EncryptingEntity(
                    secretKey,
                    cipherDetails,
                    contentEntity);

            Assert.assertThrows(IOException.class, () -> {
                retryingEntity.writeTo(output);
            });

            // clear the data written so we only see what makes it into the second request
            retryEncrypted.reset();
            output.setMinimumBytes(FailingOutputStream.NO_FAILURE);

            retryingEntity.writeTo(output);
            validateCiphertext(
                    cipherDetails,
                    secretKey,
                    contentEntity.getBackingBuffer().array(),
                    retryingEntity.getCipher().getIV(),
                    retryEncrypted.toByteArray());
        }
    }

    private void validateCiphertext(SupportedCipherDetails cipherDetails,
                                    SecretKey secretKey,
                                    byte[] plaintext,
                                    byte[] iv,
                                    byte[] ciphertext) throws IOException {
        MantaHttpHeaders responseHttpHeaders = new MantaHttpHeaders();
        responseHttpHeaders.setContentLength((long) ciphertext.length);

        if (!cipherDetails.isAEADCipher()) {
            responseHttpHeaders.put(MantaHttpHeaders.ENCRYPTION_HMAC_TYPE,
                    SupportedHmacsLookupMap.hmacNameFromInstance(cipherDetails.getAuthenticationHmac()));
        }
        responseHttpHeaders.put(MantaHttpHeaders.ENCRYPTION_IV,
                Base64.getEncoder().encodeToString(iv));
        responseHttpHeaders.put(MantaHttpHeaders.ENCRYPTION_KEY_ID,
                cipherDetails.getCipherId());

        final MantaEncryptedObjectInputStream decryptingStream = new MantaEncryptedObjectInputStream(
                new MantaObjectInputStream(
                        new MantaObjectResponse("/path", responseHttpHeaders),
                        Mockito.mock(CloseableHttpResponse.class),
                        new EofSensorInputStream(
                                new ByteArrayInputStream(ciphertext),
                                null)),
                cipherDetails,
                secretKey,
                true);

        ByteArrayOutputStream decrypted = new ByteArrayOutputStream();
        IOUtils.copy(decryptingStream, decrypted);
        decryptingStream.close();
        AssertJUnit.assertArrayEquals(plaintext, decrypted.toByteArray());
    }
}
