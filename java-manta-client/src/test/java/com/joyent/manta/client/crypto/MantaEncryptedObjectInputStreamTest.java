package com.joyent.manta.client.crypto;

import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.exception.MantaClientEncryptionCiphertextAuthenticationException;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.http.entity.MantaInputStreamEntity;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.conn.EofSensorInputStream;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

@Test
public class MantaEncryptedObjectInputStreamTest {
    private final SecureRandom random = new SecureRandom();
    private final URL testURL;
    private final int plainTextSize;
    private final byte[] plainTextBytes;

    private static class EncryptedFile {
        public final Cipher cipher;
        public final File file;

        public EncryptedFile(Cipher cipher, File file) {
            this.cipher = cipher;
            this.file = file;
        }
    }

    public MantaEncryptedObjectInputStreamTest() {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        this.testURL = classLoader.getResource("com/joyent/manta/client/crypto/MantaEncryptedObjectInputStreamTest.class");

        Path path;

        try {
            path = Paths.get(testURL.toURI());
        } catch (URISyntaxException e) {
            throw new AssertionError(e);
        }

        this.plainTextSize = (int)path.toFile().length();

        try (InputStream in = this.testURL.openStream()) {
            this.plainTextBytes = IOUtils.readFully(in, plainTextSize);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void canDecryptEntireObjectAllCiphers() throws IOException {
        for (SupportedCipherDetails cipherDetails : SupportedCiphersLookupMap.INSTANCE.values()) {
            System.out.printf("Testing decryption of [%s] as full read of stream\n",
                    cipherDetails.getCipherId());
            canDecryptEntireObject(cipherDetails);
        }
    }

    public void willErrorIfCiphertextIsModifiedAllCiphers() throws IOException {
        for (SupportedCipherDetails cipherDetails : SupportedCiphersLookupMap.INSTANCE.values()) {
            System.out.printf("Testing authentication of ciphertext with [%s] as full read of stream\n",
                    cipherDetails.getCipherId());

            boolean thrown = false;
            try {
                willThrowExceptionWhenCiphertextIsAltered(cipherDetails);
            } catch (MantaClientEncryptionCiphertextAuthenticationException e) {
                thrown = true;
                System.out.printf(" Exception thrown: %s\n", e.getMessage());
            }

            Assert.assertTrue(thrown, "Ciphertext authentication exception wasn't thrown");
        }
    }

    /* TEST UTILITY METHODS */

    private void canDecryptEntireObject(SupportedCipherDetails cipherDetails) throws IOException {
        SecretKey key = SecretKeyUtils.generate(cipherDetails);
        EncryptedFile encryptedFile = encryptedFile(key, cipherDetails);
        long ciphertextSize = encryptedFile.file.length();

        try (FileInputStream in = new FileInputStream(encryptedFile.file);
             MantaEncryptedObjectInputStream min = createEncryptedObjectInputStream(key, in,
                     ciphertextSize, cipherDetails, encryptedFile.cipher.getIV())) {

            byte[] actual = new byte[plainTextSize];
            IOUtils.readFully(min, plainTextSize);

            if (!Arrays.equals(plainTextBytes, actual)) {
                Assert.fail("Plaintext doesn't match decrypted data");
            } else {
                System.out.println(" Plaintext matched decrypted data and authentication succeeded");
            }
        }
    }

    private EncryptedFile encryptedFile(
            SecretKey key, SupportedCipherDetails cipherDetails) throws IOException {
        File temp = File.createTempFile("encrypted", ".data");
        FileUtils.forceDeleteOnExit(temp);

        try (InputStream in = testURL.openStream();
             FileOutputStream out = new FileOutputStream(temp)) {
            MantaInputStreamEntity entity = new MantaInputStreamEntity(in, plainTextSize);
            EncryptingEntity encryptingEntity = new EncryptingEntity(
                    key, cipherDetails, entity, random);
            encryptingEntity.writeTo(out);

            Assert.assertEquals(temp.length(), encryptingEntity.getContentLength(),
                    "Ciphertext doesn't equal calculated size");

            return new EncryptedFile(encryptingEntity.getCipher(), temp);
        }
    }

    private MantaEncryptedObjectInputStream createEncryptedObjectInputStream(
            SecretKey key, InputStream in, long contentLength, SupportedCipherDetails cipherDetails,
            byte[] iv) {
        String path = String.format("/test/stor/test-%s", UUID.randomUUID());
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.put(MantaHttpHeaders.ENCRYPTION_CIPHER, cipherDetails.getCipherId());
        headers.put(MantaHttpHeaders.ENCRYPTION_PLAINTEXT_CONTENT_LENGTH, plainTextSize);
        headers.put(MantaHttpHeaders.ENCRYPTION_KEY_ID, "unit-test-key");

        if (cipherDetails.isAEADCipher()) {
            headers.put(MantaHttpHeaders.ENCRYPTION_AEAD_TAG_LENGTH, cipherDetails.getAuthenticationTagOrHmacLengthInBytes());
        } else {
            headers.put(MantaHttpHeaders.ENCRYPTION_HMAC_TYPE, cipherDetails.getAuthenticationHmac().getAlgorithm());
        }

        headers.put(MantaHttpHeaders.ENCRYPTION_IV,
                Base64.getEncoder().encodeToString(iv));

        headers.setContentLength(contentLength);

        MantaMetadata metadata = new MantaMetadata();

        MantaObjectResponse response = new MantaObjectResponse(path, headers, metadata);
        CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);

        EofSensorInputStream eofSensorInputStream = new EofSensorInputStream(in, null);

        MantaObjectInputStream mantaObjectInputStream = new MantaObjectInputStream(
                response, httpResponse, eofSensorInputStream
        );

        return new MantaEncryptedObjectInputStream(mantaObjectInputStream, key);
    }

    private void willThrowExceptionWhenCiphertextIsAltered(SupportedCipherDetails cipherDetails)
            throws IOException {
        SecretKey key = SecretKeyUtils.generate(cipherDetails);
        EncryptedFile encryptedFile = encryptedFile(key, cipherDetails);

        try (FileChannel fc = (FileChannel.open(encryptedFile.file.toPath(), READ, WRITE))) {
            fc.position(2);
            ByteBuffer buff = ByteBuffer.wrap(new byte[] { 20, 20 });
            fc.write(buff);
        }

        long ciphertextSize = encryptedFile.file.length();

        try (FileInputStream in = new FileInputStream(encryptedFile.file);
             MantaEncryptedObjectInputStream min = createEncryptedObjectInputStream(key, in,
                     ciphertextSize, cipherDetails, encryptedFile.cipher.getIV())) {
            IOUtils.readFully(min, plainTextSize);
        }
    }
}
