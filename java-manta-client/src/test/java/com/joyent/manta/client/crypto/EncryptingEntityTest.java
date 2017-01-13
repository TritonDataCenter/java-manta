package com.joyent.manta.client.crypto;

import com.joyent.manta.http.entity.ExposedStringEntity;
import com.joyent.manta.http.entity.MantaInputStreamEntity;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.http.HttpEntity;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.SecretKey;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.function.Predicate;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

@Test
public class EncryptingEntityTest {
    /* AES-GCM-NoPadding Tests */

    public void canEncryptAndDecryptToAndFromFileInAesGcm() throws Exception {
        verifyEncryptionWorksRoundTrip(AesGcmCipherDetails.INSTANCE_128);
    }

    public void canEncryptAndDecryptToAndFromFileWithManySizesInAesGcm() throws Exception {
        canEncryptAndDecryptToAndFromFileWithManySizes(AesGcmCipherDetails.INSTANCE_128);
    }

    public void canCountBytesFromStreamWithUnknownLengthInAesGcm() throws Exception {
        canCountBytesFromStreamWithUnknownLength(AesGcmCipherDetails.INSTANCE_128);
    }

    @Test(expectedExceptions = AEADBadTagException.class)
    public void canEncryptAndDecryptToAndFromFileInAesGcmAndThrowWhenCiphertextIsAltered()
            throws Exception {
        SupportedCipherDetails cipherDetails = AesGcmCipherDetails.INSTANCE_128;
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URL resource = classLoader.getResource("com/joyent/manta/client/crypto/EncryptingEntityTest.class");
        Path path = Paths.get(resource.toURI());
        long size = path.toFile().length();

        MantaInputStreamEntity entity = new MantaInputStreamEntity(resource.openStream(),
                size);

        SecretKey key = SecretKeyUtils.generate(cipherDetails);
        EncryptingEntity encryptingEntity = new EncryptingEntity(key,
                cipherDetails, entity, new SecureRandom());

        File file = File.createTempFile("ciphertext-", ".data");
        FileUtils.forceDeleteOnExit(file);

        try (FileOutputStream out = new FileOutputStream(file)) {
            encryptingEntity.writeTo(out);
        }

        Assert.assertEquals(file.length(), encryptingEntity.getContentLength());

        try (FileChannel fc = (FileChannel.open(file.toPath(), READ, WRITE))) {
            fc.position(2);
            ByteBuffer buff = ByteBuffer.wrap(new byte[] { 20, 20 });
            fc.write(buff);
        }

        byte[] iv = encryptingEntity.getCipher().getIV();
        Cipher cipher = cipherDetails.getCipher();
        cipher.init(Cipher.DECRYPT_MODE, key, cipherDetails.getEncryptionParameterSpec(iv));

        try (FileInputStream in = new FileInputStream(file);
             CipherInputStream cin = new CipherInputStream(in, cipher)) {
            IOUtils.toByteArray(cin);
        } catch (IOException e) {
            Throwable cause = e.getCause();

            if (cause instanceof AEADBadTagException) {
                throw (AEADBadTagException)cause;
            } else {
                throw e;
            }
        }
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
        final Charset charset = Charsets.US_ASCII;
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
        final Charset charset = Charsets.US_ASCII;
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
                cipherDetails, entity, new SecureRandom());

        File file = File.createTempFile("ciphertext-", ".data");
        FileUtils.forceDeleteOnExit(file);

        try (FileOutputStream out = new FileOutputStream(file)) {
            encryptingEntity.writeTo(out);
        }

        Assert.assertEquals(file.length(), encryptingEntity.getContentLength(),
                "Expected ciphertext file size doesn't match actual file size -");

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
