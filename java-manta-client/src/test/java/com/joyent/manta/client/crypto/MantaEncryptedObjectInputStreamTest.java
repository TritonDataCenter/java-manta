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
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
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

    /**
     * Test that loops through all of the ciphers and attempts to decrypt an
     * encrypted stream. An assertion fails if the original plaintext and the
     * decrypted plaintext don't match. Additionally, this test tries to read
     * data from the stream using three different read methods and makes sure
     * that all read methods function correctly.
     */
    public void canDecryptEntireObjectAllCiphers() throws IOException {
        for (SupportedCipherDetails cipherDetails : SupportedCiphersLookupMap.INSTANCE.values()) {
            System.out.printf("Testing decryption of [%s] as full read of stream\n",
                    cipherDetails.getCipherId());

            System.out.println(" Reading byte by byte");
            canDecryptEntireObject(cipherDetails, new SingleReads());
            System.out.println(" Reading bytes by chunk");
            canDecryptEntireObject(cipherDetails, new ByteChunkReads());
            System.out.println(" Reading bytes by chunks with offset");
            canDecryptEntireObject(cipherDetails, new ByteChunkOffsetReads());
            System.out.println("---------------------------------");
        }
    }

    /**
     * Test that loops through all of the ciphers and attempts to decrypt an
     * encrypted stream that had its ciphertext altered. An assertion fails if
     * the underlying stream fails to throw a {@link MantaClientEncryptionCiphertextAuthenticationException}.
     * Additionally, this test tries to read data from the stream using three
     * different read methods and makes sure that all read methods function
     * correctly.
     */
    public void willErrorIfCiphertextIsModifiedAllCiphers() throws IOException {
        for (SupportedCipherDetails cipherDetails : SupportedCiphersLookupMap.INSTANCE.values()) {
            System.out.printf("Testing authentication of corrupted ciphertext with [%s] as full read of stream\n",
                    cipherDetails.getCipherId());

            System.out.println(" Reading byte by byte");
            willThrowExceptionWhenCiphertextIsAltered(cipherDetails, new SingleReads());
            System.out.println(" Reading bytes by chunk");
            willThrowExceptionWhenCiphertextIsAltered(cipherDetails, new ByteChunkReads());
            System.out.println(" Reading bytes by chunks with offset");
            willThrowExceptionWhenCiphertextIsAltered(cipherDetails, new ByteChunkOffsetReads());
        }
    }

    /**
     * Test that loops through all of the ciphers and attempts to decrypt an
     * encrypted stream that had its ciphertext altered. In this case, the
     * stream is closed before all of the bytes are read from it. An assertion
     * fails if the underlying stream fails to throw a
     * {@link MantaClientEncryptionCiphertextAuthenticationException}.
     */
    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAllCiphers() throws IOException {
        for (SupportedCipherDetails cipherDetails : SupportedCiphersLookupMap.INSTANCE.values()) {
            System.out.printf("Testing authentication of corrupted ciphertext with [%s] as partial read of stream\n",
                    cipherDetails.getCipherId());

            System.out.println(" Reading only one byte and then closing");
            willThrowExceptionWhenCiphertextIsAltered(cipherDetails, new PartialRead());
        }
    }

    /**
     * Test that loops through all of the ciphers and attempts to skip bytes
     * from an encrypted stream. This test verifies that the checksums are being
     * calculated correctly even if bytes are skipped.
     */
    public void canSkipBytesAllCiphers() throws IOException {
        for (SupportedCipherDetails cipherDetails : SupportedCiphersLookupMap.INSTANCE.values()) {
            System.out.printf("Testing authentication of ciphertext with [%s] as read and skips of stream\n",
                    cipherDetails.getCipherId());

            System.out.println(" Reading one byte, skipping 15, one byte, skipping 15");
            canReadObject(cipherDetails, new ReadSkipReadSkip());
        }
    }

    /**
     Test that loops through all of the ciphers and attempts to skip bytes from
     * an encrypted stream that had its ciphertext altered. In this case, the
     * stream is closed before all of the bytes are read from it. An assertion
     * fails if the underlying stream fails to throw a
     * {@link MantaClientEncryptionCiphertextAuthenticationException}.
     */
    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAllCiphers() throws IOException {
        for (SupportedCipherDetails cipherDetails : SupportedCiphersLookupMap.INSTANCE.values()) {
            System.out.printf("Testing authentication of corrupted ciphertext with [%s] as read and skips of stream\n",
                    cipherDetails.getCipherId());

            System.out.println(" Reading one byte, skipping 15, one byte, skipping 15");
            willThrowExceptionWhenCiphertextIsAltered(cipherDetails, new ReadSkipReadSkip());
        }
    }

    /* TEST UTILITY CLASSES */

    private static class EncryptedFile {
        public final Cipher cipher;
        public final File file;

        public EncryptedFile(Cipher cipher, File file) {
            this.cipher = cipher;
            this.file = file;
        }
    }

    private interface ReadBytes {
        int readAll(InputStream in, byte[] target) throws IOException;
    }

    private static class SingleReads implements ReadBytes {
        @Override
        public int readAll(InputStream in, byte[] target)  throws IOException {
            int totalRead = 0;
            int lastByte;

            while ((lastByte = in.read()) != -1) {
                target[totalRead++] = (byte)lastByte;
            }

            return totalRead;
        }
    }

    private static class ByteChunkReads implements ReadBytes {
        @Override
        public int readAll(InputStream in, byte[] target) throws IOException {
            int totalRead = 0;
            int lastRead;
            int chunkSize = 16;
            byte[] chunk = new byte[chunkSize];

            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                while ((lastRead = in.read(chunk)) != -1) {
                    totalRead += lastRead;
                    out.write(chunk, 0, lastRead);
                }

                byte[] written = out.toByteArray();
                for (int i = 0; i < target.length; i++) {
                    target[i] = written[i];
                }
            }

            return totalRead;
        }
    }

    private static class ByteChunkOffsetReads implements ReadBytes {
        @Override
        public int readAll(InputStream in, byte[] target) throws IOException {
            int totalRead = 0;
            int lastRead;
            int chunkSize = 128;
            byte[] chunk = new byte[512];

            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                while ((lastRead = in.read(chunk, 0, chunkSize)) != -1) {
                    totalRead += lastRead;
                    out.write(chunk, 0, lastRead);
                }

                byte[] written = out.toByteArray();
                for (int i = 0; i < target.length; i++) {
                    target[i] = written[i];
                }
            }

            return totalRead;
        }
    }

    private static class PartialRead implements ReadBytes {
        @Override
        public int readAll(InputStream in, byte[] target) throws IOException {
            int read = in.read();

            if (read == -1) {
                return -1;
            }

            target[0] = (byte)read;

            return 1;
        }
    }

    private static class ReadSkipReadSkip implements ReadBytes {
        @Override
        public int readAll(InputStream in, byte[] target) throws IOException {
            int totalRead = 0;
            int lastRead;

            if ((lastRead = in.read()) == -1) {
                return -1;
            } else {
                target[totalRead++] = (byte)lastRead;
            }

            final int skipSize = 15;
            totalRead += in.skip(skipSize);

            if ((lastRead = in.read()) == -1) {
                return -1;
            } else {
                target[totalRead++] = (byte)lastRead;
            }

            totalRead += in.skip(skipSize);

            return totalRead;
        }
    }

    /* TEST UTILITY METHODS */

    private void canDecryptEntireObject(SupportedCipherDetails cipherDetails,
                                        ReadBytes readBytes) throws IOException {
        SecretKey key = SecretKeyUtils.generate(cipherDetails);
        EncryptedFile encryptedFile = encryptedFile(key, cipherDetails);
        long ciphertextSize = encryptedFile.file.length();

        try (FileInputStream in = new FileInputStream(encryptedFile.file);
             MantaEncryptedObjectInputStream min = createEncryptedObjectInputStream(key, in,
                     ciphertextSize, cipherDetails, encryptedFile.cipher.getIV())) {

            byte[] actual = new byte[plainTextSize];
            readBytes.readAll(min, actual);

            if (!Arrays.equals(plainTextBytes, actual)) {
                Assert.fail("Plaintext doesn't match decrypted data");
            } else {
                System.out.println(" Plaintext matched decrypted data and authentication succeeded");
            }
        }
    }

    private void canReadObject(SupportedCipherDetails cipherDetails,
                               ReadBytes readBytes) throws IOException {
        SecretKey key = SecretKeyUtils.generate(cipherDetails);
        EncryptedFile encryptedFile = encryptedFile(key, cipherDetails);
        long ciphertextSize = encryptedFile.file.length();

        try (FileInputStream in = new FileInputStream(encryptedFile.file);
             MantaEncryptedObjectInputStream min = createEncryptedObjectInputStream(key, in,
                     ciphertextSize, cipherDetails, encryptedFile.cipher.getIV())) {

            byte[] actual = new byte[plainTextSize];
            readBytes.readAll(min, actual);
        }
    }

    private void willThrowExceptionWhenCiphertextIsAltered(SupportedCipherDetails cipherDetails,
                                                           ReadBytes readBytes)
            throws IOException {
        SecretKey key = SecretKeyUtils.generate(cipherDetails);
        EncryptedFile encryptedFile = encryptedFile(key, cipherDetails);

        try (FileChannel fc = (FileChannel.open(encryptedFile.file.toPath(), READ, WRITE))) {
            fc.position(2);
            ByteBuffer buff = ByteBuffer.wrap(new byte[] { 20, 20 });
            fc.write(buff);
        }

        long ciphertextSize = encryptedFile.file.length();

        boolean thrown = false;

        try (FileInputStream in = new FileInputStream(encryptedFile.file);
             MantaEncryptedObjectInputStream min = createEncryptedObjectInputStream(key, in,
                     ciphertextSize, cipherDetails, encryptedFile.cipher.getIV())) {
            byte[] actual = new byte[plainTextSize];
            readBytes.readAll(min, actual);
        }  catch (MantaClientEncryptionCiphertextAuthenticationException e) {
            thrown = true;
            System.out.printf(" Exception thrown: %s\n", e.getMessage());
        }

        Assert.assertTrue(thrown, "Ciphertext authentication exception wasn't thrown");
    }

    private EncryptedFile encryptedFile(
            SecretKey key, SupportedCipherDetails cipherDetails) throws IOException {
        File temp = File.createTempFile("encrypted", ".data");
        FileUtils.forceDeleteOnExit(temp);

        try (InputStream in = testURL.openStream();
             FileOutputStream out = new FileOutputStream(temp)) {
            MantaInputStreamEntity entity = new MantaInputStreamEntity(in, plainTextSize);
            EncryptingEntity encryptingEntity = new EncryptingEntity(
                    key, cipherDetails, entity);
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
}
