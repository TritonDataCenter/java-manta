package com.joyent.manta.client.crypto;

import com.joyent.manta.http.entity.ExposedStringEntity;
import com.joyent.manta.http.entity.MantaInputStreamEntity;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
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

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

@Test
public class EncryptingEntityTest {
    private final byte[] keyBytes;

    {
        try {
            keyBytes = "FFFFFFFBD96783C6C91E2222".getBytes("US-ASCII");
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError(e.getMessage());
        }
    }

    public void canEncryptAndDecryptToAndFromFileInAesGcm() throws Exception {
        final Charset charset = Charsets.US_ASCII;
        final String expectedString = "012345678901245601234567890124";
        SupportedCipherDetails cipherDetails = AesGcmCipherDetails.INSTANCE;
        ExposedStringEntity stringEntity = new ExposedStringEntity(
                expectedString, charset);

        SecretKey key = SecretKeyUtils.loadKey(keyBytes, cipherDetails);

        EncryptingEntity encryptingEntity = new EncryptingEntity(key,
                cipherDetails, stringEntity, new SecureRandom());

        File file = File.createTempFile("ciphertext-", ".data");
        FileUtils.forceDeleteOnExit(file);

        try (FileOutputStream out = new FileOutputStream(file)) {
            encryptingEntity.writeTo(out);
        }

        Assert.assertEquals(file.length(), encryptingEntity.getContentLength());

        byte[] iv = encryptingEntity.getCipher().getIV();
        Cipher cipher = cipherDetails.getCipher();
        cipher.init(Cipher.DECRYPT_MODE, key, cipherDetails.getEncryptionParameterSpec(iv));

        try (FileInputStream in = new FileInputStream(file);
             CipherInputStream cin = new CipherInputStream(in, cipher)) {
            final byte[] actualBytes = IOUtils.toByteArray(cin);
            final String actual = new String(actualBytes, charset);
            Assert.assertEquals(actual, expectedString,
                    "Plaintext doesn't match decrypted value");
        }
    }

    @Test(expectedExceptions = AEADBadTagException.class)
    public void canEncryptAndDecryptToAndFromFileInAesGcmAndThrowWhenCiphertextIsAltered()
            throws Exception {
        final Charset charset = Charsets.US_ASCII;
        SupportedCipherDetails cipherDetails = AesGcmCipherDetails.INSTANCE;
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URL resource = classLoader.getResource("com/joyent/manta/client/crypto/EncryptingEntityTest.class");
        Path path = Paths.get(resource.toURI());
        long size = path.toFile().length();

        MantaInputStreamEntity entity = new MantaInputStreamEntity(resource.openStream(),
                size);

        SecretKey key = SecretKeyUtils.loadKey(keyBytes, cipherDetails);

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

    public void canCountBytesFromStreamWithUnknownLength() throws Exception {
        SupportedCipherDetails cipherDetails = AesGcmCipherDetails.INSTANCE;
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URL resource = classLoader.getResource("com/joyent/manta/client/crypto/EncryptingEntityTest.class");
        Path path = Paths.get(resource.toURI());
        long size = path.toFile().length();

        MantaInputStreamEntity entity = new MantaInputStreamEntity(resource.openStream());

        Assert.assertEquals(entity.getContentLength(), -1L,
                "Content length should be set to unknown value");

        SecretKey key = SecretKeyUtils.loadKey(keyBytes, cipherDetails);

        EncryptingEntity encryptingEntity = new EncryptingEntity(key,
                cipherDetails, entity, new SecureRandom());

        File file = File.createTempFile("ciphertext-", ".data");
        FileUtils.forceDeleteOnExit(file);

        try (FileOutputStream out = new FileOutputStream(file)) {
            encryptingEntity.writeTo(out);
        }

        Assert.assertEquals(file.length(), encryptingEntity.getContentLength(),
                "Couldn't verify calculated content length");

        byte[] iv = encryptingEntity.getCipher().getIV();
        Cipher cipher = cipherDetails.getCipher();
        cipher.init(Cipher.DECRYPT_MODE, key, cipherDetails.getEncryptionParameterSpec(iv));

        try (FileInputStream in = new FileInputStream(file);
             CipherInputStream cin = new CipherInputStream(in, cipher)) {
            final byte[] actualBytes = IOUtils.toByteArray(cin);
            Assert.assertEquals(actualBytes.length, size,
                    "Incorrect number of bytes counted");
        }
    }
}
