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
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

@Test
public class MantaEncryptedObjectInputStreamTest {
    private final Path testFile;
    private final URL testURL;
    private final int plainTextSize;
    private final byte[] plainTextBytes;

    public MantaEncryptedObjectInputStreamTest() {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        this.testURL = classLoader.getResource("test-data/chaucer.txt");

        try {
            testFile = Paths.get(testURL.toURI());
        } catch (URISyntaxException e) {
            throw new AssertionError(e);
        }

        this.plainTextSize = (int)testFile.toFile().length();

        try (InputStream in = this.testURL.openStream()) {
            this.plainTextBytes = IOUtils.readFully(in, plainTextSize);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void canDecryptEntireObjectAuthenticatedAesCbc128() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_128_BIT, true);
    }

    public void canDecryptEntireObjectAuthenticatedAesCbc192() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_192_BIT, true);
    }

    public void canDecryptEntireObjectAuthenticatedAesCbc256() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_256_BIT, true);
    }

    public void canDecryptEntireObjectAuthenticatedAesCtr128() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_128_BIT, true);
    }

    public void canDecryptEntireObjectAuthenticatedAesCtr192() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_192_BIT, true);
    }

    public void canDecryptEntireObjectAuthenticatedAesCtr256() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_256_BIT, true);
    }

    public void canDecryptEntireObjectAuthenticatedAesGcm128() throws IOException {
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_128_BIT, true);
    }

    public void canDecryptEntireObjectAuthenticatedAesGcm192() throws IOException {
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_192_BIT, true);
    }

    public void canDecryptEntireObjectAuthenticatedAesGcm256() throws IOException {
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_256_BIT, true);
    }


    public void canDecryptEntireObjectUnauthenticatedAesCbc128() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_128_BIT, false);
    }

    public void canDecryptEntireObjectUnauthenticatedAesCbc192() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_192_BIT, false);
    }

    public void canDecryptEntireObjectUnauthenticatedAesCbc256() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_256_BIT, false);
    }

    public void canDecryptEntireObjectUnauthenticatedAesCtr128() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_128_BIT, false);
    }

    public void canDecryptEntireObjectUnauthenticatedAesCtr192() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_192_BIT, false);
    }

    public void canDecryptEntireObjectUnauthenticatedAesCtr256() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_256_BIT, false);
    }

    public void canDecryptEntireObjectUnauthenticatedAesGcm128() throws IOException {
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_128_BIT, false);
    }

    public void canDecryptEntireObjectUnauthenticatedAesGcm192() throws IOException {
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_192_BIT, false);
    }

    public void canDecryptEntireObjectUnauthenticatedAesGcm256() throws IOException {
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_256_BIT, false);
    }


    public void willErrorIfCiphertextIsModifiedAesCbc128() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesCbcCipherDetails.INSTANCE_128_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAesCbc192() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesCbcCipherDetails.INSTANCE_192_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAesCbc256() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesCbcCipherDetails.INSTANCE_256_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAesCtr128() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAesCtr192() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesCtrCipherDetails.INSTANCE_192_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAesCtr256() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesCtrCipherDetails.INSTANCE_256_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAesGcm128() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesGcmCipherDetails.INSTANCE_128_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAesGcm192() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesGcmCipherDetails.INSTANCE_192_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAesGcm256() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesGcmCipherDetails.INSTANCE_256_BIT);
    }


    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesCbc128() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesCbcCipherDetails.INSTANCE_128_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesCbc192() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesCbcCipherDetails.INSTANCE_192_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesCbc256() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesCbcCipherDetails.INSTANCE_256_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesCtr128() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesCtr192() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesCtrCipherDetails.INSTANCE_192_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesCtr256() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesCtrCipherDetails.INSTANCE_256_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesGcm128() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesGcmCipherDetails.INSTANCE_128_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesGcm192() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesGcmCipherDetails.INSTANCE_192_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesGcm256() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesGcmCipherDetails.INSTANCE_256_BIT);
    }


    public void canSkipBytesAuthenticatedAesCbc128() throws IOException {
        canSkipBytesAuthenticated(AesCbcCipherDetails.INSTANCE_128_BIT);
    }

    public void canSkipBytesAuthenticatedAesCbc192() throws IOException {
        canSkipBytesAuthenticated(AesCbcCipherDetails.INSTANCE_192_BIT);
    }

    public void canSkipBytesAuthenticatedAesCbc256() throws IOException {
        canSkipBytesAuthenticated(AesCbcCipherDetails.INSTANCE_256_BIT);
    }

    public void canSkipBytesAuthenticatedAesCtr128() throws IOException {
        canSkipBytesAuthenticated(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    public void canSkipBytesAuthenticatedAesCtr192() throws IOException {
        canSkipBytesAuthenticated(AesCtrCipherDetails.INSTANCE_192_BIT);
    }

    public void canSkipBytesAuthenticatedAesCtr256() throws IOException {
        canSkipBytesAuthenticated(AesCtrCipherDetails.INSTANCE_256_BIT);
    }

    public void canSkipBytesAuthenticatedAesGcm128() throws IOException {
        canSkipBytesAuthenticated(AesGcmCipherDetails.INSTANCE_128_BIT);
    }

    public void canSkipBytesAuthenticatedAesGcm192() throws IOException {
        canSkipBytesAuthenticated(AesGcmCipherDetails.INSTANCE_192_BIT);
    }

    public void canSkipBytesAuthenticatedAesGcm256() throws IOException {
        canSkipBytesAuthenticated(AesGcmCipherDetails.INSTANCE_256_BIT);
    }


    public void canSkipBytesUnauthenticatedAesCbc128() throws IOException {
        canSkipBytesUnauthenticated(AesCbcCipherDetails.INSTANCE_128_BIT);
    }

    public void canSkipBytesUnauthenticatedAesCbc192() throws IOException {
        canSkipBytesUnauthenticated(AesCbcCipherDetails.INSTANCE_192_BIT);
    }

    public void canSkipBytesUnauthenticatedAesCbc256() throws IOException {
        canSkipBytesUnauthenticated(AesCbcCipherDetails.INSTANCE_256_BIT);
    }

    public void canSkipBytesUnauthenticatedAesCtr128() throws IOException {
        canSkipBytesUnauthenticated(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    public void canSkipBytesUnauthenticatedAesCtr192() throws IOException {
        canSkipBytesUnauthenticated(AesCtrCipherDetails.INSTANCE_192_BIT);
    }

    public void canSkipBytesUnauthenticatedAesCtr256() throws IOException {
        canSkipBytesUnauthenticated(AesCtrCipherDetails.INSTANCE_256_BIT);
    }

    public void canSkipBytesUnauthenticatedAesGcm128() throws IOException {
        canSkipBytesUnauthenticated(AesGcmCipherDetails.INSTANCE_128_BIT);
    }

    public void canSkipBytesUnauthenticatedAesGcm192() throws IOException {
        canSkipBytesUnauthenticated(AesGcmCipherDetails.INSTANCE_192_BIT);
    }

    public void canSkipBytesUnauthenticatedAesGcm256() throws IOException {
        canSkipBytesUnauthenticated(AesGcmCipherDetails.INSTANCE_256_BIT);
    }


    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesCbc128() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesCbcCipherDetails.INSTANCE_128_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesCbc192() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesCbcCipherDetails.INSTANCE_192_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesCbc256() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesCbcCipherDetails.INSTANCE_256_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesCtr128() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesCtr192() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesCtrCipherDetails.INSTANCE_192_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesCtr256() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesCtrCipherDetails.INSTANCE_256_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesGcm128() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesGcmCipherDetails.INSTANCE_128_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesGcm192() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesGcmCipherDetails.INSTANCE_192_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesGcm256() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesGcmCipherDetails.INSTANCE_256_BIT);
    }


    public void canReadByteRangeStartingAtZeroEndingInFirstBlockAesCtr128() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() / 2;
        canReadByteRangeAllReadModes(cipherDetails, 0, endPos);
    }

    public void canReadByteRangeStartingAtZeroEndingInFirstBlockAesCtr192() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_192_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() / 2;
        canReadByteRangeAllReadModes(cipherDetails, 0, endPos);
    }

    public void canReadByteRangeStartingAtZeroEndingInFirstBlockAesCtr256() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_256_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() / 2;
        canReadByteRangeAllReadModes(cipherDetails, 0, endPos);
    }


    public void canReadByteRangeStartingAtZeroEndingInThirdBlockAesCtr128() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() * 2 + (cipherDetails.getBlockSizeInBytes() / 2);
        canReadByteRangeAllReadModes(cipherDetails, 0, endPos);
    }

    public void canReadByteRangeStartingAtZeroEndingInThirdBlockAesCtr192() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_192_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() * 2 + (cipherDetails.getBlockSizeInBytes() / 2);
        canReadByteRangeAllReadModes(cipherDetails, 0, endPos);
    }

    public void canReadByteRangeStartingAtZeroEndingInThirdBlockAesCtr256() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_256_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() * 2 + (cipherDetails.getBlockSizeInBytes() / 2);
        canReadByteRangeAllReadModes(cipherDetails, 0, endPos);
    }


    public void canReadByteRangeStartingAtThreeEndingInFirstBlockAesCtr128() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() / 2;
        canReadByteRangeAllReadModes(cipherDetails, 3, endPos);
    }

    public void canReadByteRangeStartingAtThreeEndingInFirstBlockAesCtr192() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_192_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() / 2;
        canReadByteRangeAllReadModes(cipherDetails, 3, endPos);
    }

    public void canReadByteRangeStartingAtThreeEndingInFirstBlockAesCtr256() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_256_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() / 2;
        canReadByteRangeAllReadModes(cipherDetails, 3, endPos);
    }

    public void canReadByteRangeStartingAtThirdBlockEndingInFifthBlockAesCtr128() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
        int startPos = cipherDetails.getBlockSizeInBytes() * 3 + (cipherDetails.getBlockSizeInBytes() / 2);
        int endPos = cipherDetails.getBlockSizeInBytes() * 5 + (cipherDetails.getBlockSizeInBytes() / 2);
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
    }

    public void canReadByteRangeStartingAtThirdBlockEndingInFifthBlockAesCtr192() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_192_BIT;
        int startPos = cipherDetails.getBlockSizeInBytes() * 3 + (cipherDetails.getBlockSizeInBytes() / 2);
        int endPos = cipherDetails.getBlockSizeInBytes() * 5 + (cipherDetails.getBlockSizeInBytes() / 2);
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
    }

    public void canReadByteRangeStartingAtThirdBlockEndingInFifthBlockAesCtr256() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_256_BIT;
        int startPos = cipherDetails.getBlockSizeInBytes() * 3 + (cipherDetails.getBlockSizeInBytes() / 2);
        int endPos = cipherDetails.getBlockSizeInBytes() * 5 + (cipherDetails.getBlockSizeInBytes() / 2);
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
    }

    public void canReadByteRangeStarting25bytesFromEndToEndAesCtr128() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
        int endPos = plainTextSize - 1;
        int startPos = endPos - 25;
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
    }

    public void canReadByteRangeStarting25bytesFromEndToEndAesCtr192() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_192_BIT;
        int endPos = plainTextSize - 1;
        int startPos = endPos - 25;
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
    }

    public void canReadByteRangeStarting25bytesFromEndToEndAesCtr256() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_256_BIT;
        int endPos = plainTextSize - 1;
        int startPos = endPos - 25;
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
    }

    public void canReadByteRangeStarting25bytesFromEndToBeyondEndAesCtr128() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
        int endPos = plainTextSize * 2;
        int startPos = plainTextSize - 26;
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
    }

    public void canReadByteRangeStarting25bytesFromEndToBeyondEndAesCtr192() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_192_BIT;
        int endPos = plainTextSize * 2;
        int startPos = plainTextSize - 26;
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
    }

    public void canReadByteRangeStarting25bytesFromEndToBeyondEndAesCtr256() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_256_BIT;
        int endPos = plainTextSize * 2;
        int startPos = plainTextSize - 26;
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
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

            try {
                while ((lastByte = in.read()) != -1) {
                    target[totalRead++] = (byte) lastByte;
                }
            } catch (ArrayIndexOutOfBoundsException e) {
                while (in.read() != -1) {
                    totalRead++;
                }

                String msg = String.format("%d bytes available in stream, but "
                        + "the byte array target has a length of %d bytes",
                        totalRead, target.length);
                throw new AssertionError(msg, e);
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
                System.arraycopy(written, 0, target, 0, target.length);
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
                System.arraycopy(written, 0, target, 0, target.length);
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

    /**
     * Test that loops through all of the ciphers and attempts to decrypt an
     * encrypted stream. An assertion fails if the original plaintext and the
     * decrypted plaintext don't match. Additionally, this test tries to read
     * data from the stream using three different read methods and makes sure
     * that all read methods function correctly.
     */
    private void canDecryptEntireObjectAllReadModes(SupportedCipherDetails cipherDetails, boolean authenticate) throws IOException {
        System.out.printf("Testing decryption of [%s] as full read of stream\n",
                cipherDetails.getCipherId());

        System.out.println(" Reading byte by byte");
        canDecryptEntireObject(cipherDetails, new SingleReads(), authenticate);
        System.out.println(" Reading bytes by chunk");
        canDecryptEntireObject(cipherDetails, new ByteChunkReads(), authenticate);
        System.out.println(" Reading bytes by chunks with offset");
        canDecryptEntireObject(cipherDetails, new ByteChunkOffsetReads(), authenticate);
        System.out.println("---------------------------------");
    }

    /**
     * Test that loops through all of the ciphers and attempts to decrypt an
     * encrypted stream that had its ciphertext altered. An assertion fails if
     * the underlying stream fails to throw a {@link MantaClientEncryptionCiphertextAuthenticationException}.
     * Additionally, this test tries to read data from the stream using three
     * different read methods and makes sure that all read methods function
     * correctly.
     */
    private void willErrorIfCiphertextIsModifiedAllReadModes(SupportedCipherDetails cipherDetails) throws IOException {
        System.out.printf("Testing authentication of corrupted ciphertext with [%s] as full read of stream\n",
                cipherDetails.getCipherId());

        System.out.println(" Reading byte by byte");
        willThrowExceptionWhenCiphertextIsAltered(cipherDetails, new SingleReads());
        System.out.println(" Reading bytes by chunk");
        willThrowExceptionWhenCiphertextIsAltered(cipherDetails, new ByteChunkReads());
        System.out.println(" Reading bytes by chunks with offset");
        willThrowExceptionWhenCiphertextIsAltered(cipherDetails, new ByteChunkOffsetReads());
    }

    /**
     * Test that loops through all of the ciphers and attempts to decrypt an
     * encrypted stream that had its ciphertext altered. In this case, the
     * stream is closed before all of the bytes are read from it. An assertion
     * fails if the underlying stream fails to throw a
     * {@link MantaClientEncryptionCiphertextAuthenticationException}.
     */
    private void willErrorIfCiphertextIsModifiedAndNotReadFully(SupportedCipherDetails cipherDetails) throws IOException {
        System.out.printf("Testing authentication of corrupted ciphertext with [%s] as partial read of stream\n",
                cipherDetails.getCipherId());

        System.out.println(" Reading only one byte and then closing");
        willThrowExceptionWhenCiphertextIsAltered(cipherDetails, new PartialRead());
    }

    /**
     * Test that loops through all of the ciphers and attempts to skip bytes
     * from an encrypted stream. This test verifies that the checksums are being
     * calculated correctly even if bytes are skipped.
     */
    private void canSkipBytesAuthenticated(SupportedCipherDetails cipherDetails) throws IOException {
        System.out.printf("Testing authentication of ciphertext with [%s] as read and skips of stream\n",
                cipherDetails.getCipherId());

        System.out.println(" Reading one byte, skipping 15, one byte, skipping 15");
        canReadObject(cipherDetails, new ReadSkipReadSkip(), true);
    }

    /**
     * Test that loops through all of the ciphers and attempts to skip bytes
     * from an encrypted stream.
     */
    private void canSkipBytesUnauthenticated(SupportedCipherDetails cipherDetails) throws IOException {
        System.out.printf("Testing authentication of ciphertext with [%s] as read and skips of stream\n",
                cipherDetails.getCipherId());

        System.out.println(" Reading one byte, skipping 15, one byte, skipping 15");
        canReadObject(cipherDetails, new ReadSkipReadSkip(), false);
    }

    private void canDecryptEntireObject(SupportedCipherDetails cipherDetails,
                                        ReadBytes readBytes, boolean authenticate) throws IOException {
        SecretKey key = SecretKeyUtils.generate(cipherDetails);
        EncryptedFile encryptedFile = encryptedFile(key, cipherDetails);
        long ciphertextSize = encryptedFile.file.length();

        try (FileInputStream in = new FileInputStream(encryptedFile.file);
             MantaEncryptedObjectInputStream min = createEncryptedObjectInputStream(key, in,
                     ciphertextSize, cipherDetails, encryptedFile.cipher.getIV(), authenticate)) {

            byte[] actual = new byte[plainTextSize];
            readBytes.readAll(min, actual);

            AssertJUnit.assertArrayEquals("Plaintext doesn't match decrypted data", plainTextBytes, actual);
            System.out.println(" Plaintext matched decrypted data and authentication succeeded");
        }
    }

    /**
     * Test that attempts to skip bytes from an encrypted stream that had its
     * ciphertext altered. In this case, the stream is closed before all of the
     * bytes are read from it. An assertion fails if the underlying stream
     * fails to throw a {@link MantaClientEncryptionCiphertextAuthenticationException}.
     */
    private void willErrorIfCiphertextIsModifiedAndBytesAreSkipped(SupportedCipherDetails cipherDetails) throws IOException {
        System.out.printf("Testing authentication of corrupted ciphertext with [%s] as read and skips of stream\n",
                cipherDetails.getCipherId());

        System.out.println(" Reading one byte, skipping 15, one byte, skipping 15");
        willThrowExceptionWhenCiphertextIsAltered(cipherDetails, new ReadSkipReadSkip());
    }

    /**
     * Test that attempts to read a byte range from an encrypted stream starting
     * at the first byte.
     */
    private void canReadByteRangeAllReadModes(SupportedCipherDetails cipherDetails,
                                              int startPosInclusive,
                                              int endPosInclusive)
            throws IOException {
        System.out.printf("Testing byte range read starting from zero for cipher [%s] as full read of "
                        + "truncated stream\n",
                cipherDetails.getCipherId());

        System.out.println(" Reading byte by byte");
        canReadByteRange(cipherDetails, new SingleReads(), startPosInclusive, endPosInclusive);
        System.out.println(" Reading bytes by chunk");
        canReadByteRange(cipherDetails, new ByteChunkReads(), startPosInclusive, endPosInclusive);
        System.out.println(" Reading bytes by chunks with offset");
        canReadByteRange(cipherDetails, new ByteChunkOffsetReads(), startPosInclusive, endPosInclusive);
    }

    private void canReadByteRange(SupportedCipherDetails cipherDetails,
                                  ReadBytes readBytes,
                                  int startPosInclusive,
                                  int endPosInclusive) throws IOException {
        final byte[] content;
        try (InputStream in = testURL.openStream();
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            IOUtils.copy(in, out);
            content = out.toByteArray();
        }

        /* If the plaintext size specified is greater than the actual plaintext
         * size, we adjust it here. This is only for creating the expectation
         * that we compare our output to. */
        final int endPositionExclusive;

        if (endPosInclusive + 1 > plainTextSize) {
            endPositionExclusive = plainTextSize;
        } else {
            endPositionExclusive = endPosInclusive + 1;
        }

        byte[] expected = Arrays.copyOfRange(content, startPosInclusive, endPositionExclusive);

        SecretKey key = SecretKeyUtils.generate(cipherDetails);
        EncryptedFile encryptedFile = encryptedFile(key, cipherDetails);
        long ciphertextSize = encryptedFile.file.length();

        /* Here we translate the plaintext ranges to ciphertext ranges. Notice
         * that we take the input of endPos because it may overrun past the
         * size of the plaintext. */
        long[] ranges = cipherDetails.translateByteRange(startPosInclusive, endPosInclusive);
        long ciphertextStart = ranges[0];
        long plaintextLength = ranges[3];

        /* Here, we simulate being passed only a subset of the total bytes of
         * a file - just like how the output of a HTTP range request would be
         * passed. */
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(encryptedFile.file, "r")) {
            // We seek to the start of the ciphertext to emulate a HTTP byte range request
            randomAccessFile.seek(ciphertextStart);
            /* We then calculate the range length based on the *actual* ciphertext
             * size so that we are emulating HTTP range requests by returning a
             * the binary of the actual object (even if the range went beyond). */
            long ciphertextByteRangeLength = ciphertextSize - ciphertextStart;
            byte[] rangedBytes = new byte[(int)ciphertextByteRangeLength];
            randomAccessFile.read(rangedBytes);

            try (ByteArrayInputStream bin = new ByteArrayInputStream(rangedBytes);
                /* When creating the fake stream, we feed it a content-length equal
                 * to the size of the byte range because that is what Manta would
                 * do. Also, we pass along the incorrect plaintext length and
                 * test how the stream handles incorrect values of plaintext length. */
                 MantaEncryptedObjectInputStream min = createEncryptedObjectInputStream(key, bin,
                         ciphertextByteRangeLength, cipherDetails, encryptedFile.cipher.getIV(),
                    false, Integer.valueOf(startPosInclusive).longValue(),
                         plaintextLength)) {
                byte[] actual = new byte[expected.length];
                readBytes.readAll(min, actual);

                try {
                    AssertJUnit.assertArrayEquals("Byte range output doesn't match",
                            expected, actual);
                } catch (AssertionError e) {
                    Assert.fail(String.format("%s\nexpected: %s\nactual  : %s",
                            e.getMessage(), new String(expected), new String(actual)));
                }
            }
        }
    }

    private void canReadObject(SupportedCipherDetails cipherDetails,
                               ReadBytes readBytes,
                               boolean authenticate) throws IOException {
        SecretKey key = SecretKeyUtils.generate(cipherDetails);
        EncryptedFile encryptedFile = encryptedFile(key, cipherDetails);
        long ciphertextSize = encryptedFile.file.length();

        try (FileInputStream in = new FileInputStream(encryptedFile.file);
             MantaEncryptedObjectInputStream min = createEncryptedObjectInputStream(key, in,
                     ciphertextSize, cipherDetails, encryptedFile.cipher.getIV(),
                     authenticate)) {

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
                     ciphertextSize, cipherDetails, encryptedFile.cipher.getIV(), true)) {
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
            byte[] iv, boolean authenticate) {
        return createEncryptedObjectInputStream(key, in, contentLength, cipherDetails, iv,
                authenticate, null, null);
    }

    private MantaEncryptedObjectInputStream createEncryptedObjectInputStream(
            SecretKey key, InputStream in, long contentLength, SupportedCipherDetails cipherDetails,
            byte[] iv, boolean authenticate, Long startPos, Long plaintextLength) {
        String path = String.format("/test/stor/test-%s", UUID.randomUUID());
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.put(MantaHttpHeaders.ENCRYPTION_CIPHER, cipherDetails.getCipherId());
        headers.put(MantaHttpHeaders.ENCRYPTION_PLAINTEXT_CONTENT_LENGTH, this.plainTextSize);
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

        return new MantaEncryptedObjectInputStream(mantaObjectInputStream,
                cipherDetails,key, authenticate,
                startPos, plaintextLength);
    }
}
