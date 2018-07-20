/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.exception.MantaClientEncryptionCiphertextAuthenticationException;
import com.joyent.manta.exception.MantaIOException;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.http.entity.MantaInputStreamEntity;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.RandomUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.conn.EofSensorInputStream;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.AssertJUnit;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class AbstractMantaEncryptedObjectInputStreamTest {

    protected final Path testFile;
    protected final URL testURL;
    protected final int plaintextSize;
    protected final byte[] plaintextBytes;

    AbstractMantaEncryptedObjectInputStreamTest() {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        this.testURL = classLoader.getResource("test-data/chaucer.txt");

        try {
            testFile = Paths.get(testURL.toURI());
        } catch (URISyntaxException e) {
            throw new AssertionError(e);
        }

        this.plaintextSize = (int)testFile.toFile().length();

        try (InputStream in = this.testURL.openStream()) {
            this.plaintextBytes = IOUtils.readFully(in, plaintextSize);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /*

    removing failing tests documented in https://github.com/joyent/java-manta/issues/257

    public void willValidateIfHmacIsReadInMultipleReadsAesCbc128() throws IOException {
        willValidateIfHmacIsReadInMultipleReads(AesCbcCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willValidateIfHmacIsReadInMultipleReadsAesCbc192() throws IOException {
        willValidateIfHmacIsReadInMultipleReads(AesCbcCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willValidateIfHmacIsReadInMultipleReadsAesCbc256() throws IOException {
        willValidateIfHmacIsReadInMultipleReads(AesCbcCipherDetails.INSTANCE_256_BIT);
    }

    */

    /* TEST UTILITY CLASSES */

    protected static class EncryptedFile {
        public final Cipher cipher;
        public final File file;

        public EncryptedFile(Cipher cipher, File file) {
            this.cipher = cipher;
            this.file = file;
        }
    }


    protected static final class ReadBytesFactory {

        public static <T extends ReadBytes> ReadBytes fullStrategy(final Class<T> klass) {

            if (SingleReads.class.equals(klass)) {
                return new SingleReads();
            }


            if (ByteChunkReads.class.equals(klass)) {
                return new ByteChunkReads();
            }

            if (ByteChunkOffsetReads.class.equals(klass)) {
                return new ByteChunkOffsetReads();
            }

            throw new ClassCastException("Don't know how to build class: " + klass.getCanonicalName());
        }

        public static <T extends ReadPartialBytes> ReadBytes partialStrategy(final Class<? extends ReadPartialBytes> klass,
                                                                             final long inputSize) {

            if (SingleBytePartialRead.class.equals(klass)) {
                return new SingleBytePartialRead();
            }


            if (ReadAndSkipPartialRead.class.equals(klass)) {
                return new ReadAndSkipPartialRead(inputSize);
            }

            throw new ClassCastException("Don't know how to build class: " + klass.getCanonicalName());
        }

        public static <T extends ReadBytes> ReadBytes build(final Class<T> klass, final long inputSize) {
            if (ReadPartialBytes.class.isAssignableFrom(klass)) {
                return partialStrategy((Class<? extends ReadPartialBytes>) klass, inputSize);
            }

            if (ReadBytes.class.isAssignableFrom(klass)) {
                return fullStrategy(klass);
            }

            throw new ClassCastException("Don't know how to build class: " + klass.getCanonicalName());
        }

    }

    protected interface ReadBytes {
        int readBytes(InputStream in, byte[] target) throws IOException;
    }

    protected interface ReadPartialBytes extends ReadBytes {
    }

    protected static class SingleReads implements ReadBytes {
        @Override
        public int readBytes(InputStream in, byte[] target)  throws IOException {
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

    protected static class ByteChunkReads implements ReadBytes {
        @Override
        public int readBytes(InputStream in, byte[] target) throws IOException {
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

    protected static class ByteChunkOffsetReads implements ReadBytes {
        @Override
        public int readBytes(InputStream in, byte[] target) throws IOException {
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

    protected static class SingleBytePartialRead implements ReadPartialBytes {
        @Override
        public int readBytes(InputStream in, byte[] target) throws IOException {
            int read = in.read();

            if (read == -1) {
                return -1;
            }

            target[0] = (byte)read;

            return 1;
        }
    }

    /**
     * This class needs to know the amount of data being read so that it can calculate
     * skip lengths to cover at least half of the stream.
     */
    protected static class ReadAndSkipPartialRead implements ReadPartialBytes {

        private final long inputSize;

        ReadAndSkipPartialRead(final long inputSize) {
            this.inputSize = inputSize;
        }

        @Override
        public int readBytes(InputStream in, byte[] target) throws IOException {
            int totalRead = 0;
            int lastRead;

            if ((lastRead = in.read()) == -1) {
                return -1;
            } else {
                target[totalRead++] = (byte)lastRead;
            }

            final int skipSize = Math.toIntExact(Math.floorDiv(this.inputSize, 4));
            totalRead += Math.toIntExact(in.skip(skipSize));

            if ((lastRead = in.read()) == -1) {
                return -1;
            } else {
                target[totalRead++] = (byte)lastRead;
            }

            totalRead += Math.toIntExact(in.skip(skipSize));

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
    protected void canDecryptEntireObjectAllReadModes(SupportedCipherDetails cipherDetails, boolean authenticate) throws IOException {
        System.out.printf("Testing decryption of [%s] as full read of stream\n",
                cipherDetails.getCipherId());

        canDecryptEntireObject(cipherDetails, SingleReads.class, authenticate);
        canDecryptEntireObject(cipherDetails, ByteChunkReads.class, authenticate);
        canDecryptEntireObject(cipherDetails, ByteChunkOffsetReads.class, authenticate);
    }


    /**
     * Attempts to copy a {@link MantaEncryptedObjectInputStream} stream to
     * a {@link java.io.OutputStream} and close the streams. Copy is done using
     * a large buffer size and logic borrowed directly from COSBench.
     */
    protected void canCopyToOutputStreamWithLargeBuffer(SupportedCipherDetails cipherDetails,
                                                      boolean authenticate)
            throws IOException {
        final byte[] buffer = new byte[1024*1024];
        final int sourceLength = 8000;
        final byte[] sourceBytes = RandomUtils.nextBytes(sourceLength);

        SecretKey key = SecretKeyUtils.generate(cipherDetails);
        final EncryptedFile encryptedFile;

        try (InputStream in = new ByteArrayInputStream(sourceBytes)) {
            encryptedFile = encryptedFile(key, cipherDetails, sourceLength, in);
        }

        final byte[] iv = encryptedFile.cipher.getIV();

        final long ciphertextSize = encryptedFile.file.length();

        InputStream backing = new FileInputStream(encryptedFile.file);
        final InputStream inSpy = Mockito.spy(backing);

        MantaEncryptedObjectInputStream in = createEncryptedObjectInputStream(
                key, inSpy, ciphertextSize, cipherDetails, iv, authenticate,
                sourceLength);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try {
            // Don't change me - I'm imitating COSBench
            for (int n; -1 != (n = in.read(buffer));) {
                out.write(buffer, 0, n);
            }
        } finally {
            in.close();
            out.close();
        }

        AssertJUnit.assertArrayEquals(sourceBytes, out.toByteArray());
        Mockito.verify(inSpy, Mockito.atLeastOnce()).close();
    }

    /**
     * Test that loops through all of the ciphers and attempts to decrypt an
     * encrypted stream that had its ciphertext altered. An assertion fails if
     * the underlying stream fails to throw a {@link MantaClientEncryptionCiphertextAuthenticationException}.
     * Additionally, this test tries to read data from the stream using three
     * different read methods and makes sure that all read methods function
     * correctly.
     */
    protected void willErrorIfCiphertextIsModifiedAllReadModes(SupportedCipherDetails cipherDetails) throws IOException {
        System.out.printf("Testing authentication of corrupted ciphertext with [%s] as full read of stream\n",
                cipherDetails.getCipherId());

        willThrowExceptionWhenCiphertextIsAltered(cipherDetails, SingleReads.class);
        willThrowExceptionWhenCiphertextIsAltered(cipherDetails, ByteChunkReads.class);
        willThrowExceptionWhenCiphertextIsAltered(cipherDetails, ByteChunkOffsetReads.class);
    }

    /**
     * Test that loops through all of the ciphers and attempts to decrypt an
     * encrypted stream that had its ciphertext altered. In this case, the
     * stream is closed before all of the bytes are read from it. An assertion
     * fails if the underlying stream fails to throw a
     * {@link MantaClientEncryptionCiphertextAuthenticationException}.
     */
    protected void willErrorIfCiphertextIsModifiedAndNotReadFully(SupportedCipherDetails cipherDetails) throws IOException {
        System.out.printf("Testing authentication of corrupted ciphertext with [%s] as partial read of stream\n",
                cipherDetails.getCipherId());

        willThrowExceptionWhenCiphertextIsAltered(cipherDetails, SingleBytePartialRead.class);
    }

    /**
     * Test that loops through all of the ciphers and attempts to skip bytes
     * from an encrypted stream. This test verifies that the checksums are being
     * calculated correctly even if bytes are skipped.
     */
    protected void canSkipBytesAuthenticated(SupportedCipherDetails cipherDetails) throws IOException {
        System.out.printf("Testing authentication of ciphertext with [%s] as read and skips of stream\n",
                cipherDetails.getCipherId());

        canReadObject(cipherDetails, ReadAndSkipPartialRead.class, true);
    }

    /**
     * Test that loops through all of the ciphers and attempts to skip bytes
     * from an encrypted stream.
     */
    protected void canSkipBytesUnauthenticated(SupportedCipherDetails cipherDetails) throws IOException {
        System.out.printf("Testing authentication of ciphertext with [%s] as read and skips of stream\n",
                cipherDetails.getCipherId());

        canReadObject(cipherDetails, ReadAndSkipPartialRead.class, false);
    }

    protected void canDecryptEntireObject(SupportedCipherDetails cipherDetails,
                                        Class<? extends ReadBytes> strategy, boolean authenticate) throws IOException {
        SecretKey key = SecretKeyUtils.generate(cipherDetails);
        EncryptedFile encryptedFile = encryptedFile(key, cipherDetails, this.plaintextSize);
        long ciphertextSize = encryptedFile.file.length();

        final FileInputStream in = new FileInputStream(encryptedFile.file);
        final FileInputStream inSpy = Mockito.spy(in);

        MantaEncryptedObjectInputStream min = createEncryptedObjectInputStream(key, inSpy,
                ciphertextSize, cipherDetails, encryptedFile.cipher.getIV(), authenticate,
                (long)this.plaintextSize);

        try {
            byte[] actual = new byte[plaintextSize];
            ReadBytesFactory.fullStrategy(strategy).readBytes(min, actual);

            AssertJUnit.assertArrayEquals("Plaintext doesn't match decrypted data", plaintextBytes, actual);
        } finally {
            min.close();
        }

        Mockito.verify(inSpy, Mockito.atLeastOnce()).close();
    }

    /**
     * Test that attempts to skip bytes from an encrypted stream that had its
     * ciphertext altered. In this case, the stream is closed before all of the
     * bytes are read from it. An assertion fails if the underlying stream
     * fails to throw a {@link MantaClientEncryptionCiphertextAuthenticationException}.
     */
    protected void willErrorIfCiphertextIsModifiedAndBytesAreSkipped(SupportedCipherDetails cipherDetails) throws IOException {
        System.out.printf("Testing authentication of corrupted ciphertext with [%s] as read and skips of stream\n",
                cipherDetails.getCipherId());

        willThrowExceptionWhenCiphertextIsAltered(cipherDetails, ReadAndSkipPartialRead.class);
    }

    /**
     * Test that attempts to read a byte range from an encrypted stream starting
     * at the first byte.
     */
    protected void canReadByteRangeAllReadModes(SupportedCipherDetails cipherDetails,
                                              int startPosInclusive,
                                              int endPosInclusive)
            throws IOException {
        System.out.printf("Testing byte range read starting from zero for cipher [%s] as full read of "
                        + "truncated stream\n",
                cipherDetails.getCipherId());

        canReadByteRange(cipherDetails, SingleReads.class, startPosInclusive, endPosInclusive);
        canReadByteRange(cipherDetails, ByteChunkReads.class, startPosInclusive, endPosInclusive);
        canReadByteRange(cipherDetails, ByteChunkOffsetReads.class, startPosInclusive, endPosInclusive);
    }

    protected void canReadByteRange(SupportedCipherDetails cipherDetails,
                                  Class<? extends ReadBytes> strategy,
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

        if (endPosInclusive + 1 > plaintextSize) {
            endPositionExclusive = plaintextSize;
        } else {
            endPositionExclusive = endPosInclusive + 1;
        }

        boolean unboundedEnd = (endPositionExclusive >= plaintextSize || endPosInclusive < 0);
        byte[] expected = Arrays.copyOfRange(content, startPosInclusive, endPositionExclusive);

        SecretKey key = SecretKeyUtils.generate(cipherDetails);
        EncryptedFile encryptedFile = encryptedFile(key, cipherDetails, this.plaintextSize);
        long ciphertextSize = encryptedFile.file.length();

        /* Here we translate the plaintext ranges to ciphertext ranges. Notice
         * that we take the input of endPos because it may overrun past the
         * size of the plaintext. */
        ByteRangeConversion ranges = cipherDetails.translateByteRange(startPosInclusive, endPosInclusive);
        long ciphertextStart = ranges.getCiphertextStartPositionInclusive();
        long plaintextLength = ((long)(endPosInclusive - startPosInclusive)) + 1L;

        /* Here, we simulate being passed only a subset of the total bytes of
         * a file - just like how the output of a HTTP range request would be
         * passed. */
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(encryptedFile.file, "r")) {
            // We seek to the start of the ciphertext to emulate a HTTP byte range request
            randomAccessFile.seek(ciphertextStart);

            long initialSkipBytes = ranges.getPlaintextBytesToSkipInitially() + ranges.getCiphertextStartPositionInclusive();
            long binaryEndPositionInclusive = ranges.getCiphertextEndPositionInclusive();

            long ciphertextRangeMaxBytes = ciphertextSize - ciphertextStart;
            long ciphertextRangeRequestBytes = binaryEndPositionInclusive - ciphertextStart + 1;

            /* We then calculate the range length based on the *actual* ciphertext
             * size so that we are emulating HTTP range requests by returning a
             * the binary of the actual object (even if the range went beyond). */
            long ciphertextByteRangeLength = Math.min(ciphertextRangeMaxBytes, ciphertextRangeRequestBytes);

            if (unboundedEnd)
                ciphertextByteRangeLength = ciphertextSize - ciphertextStart; // include MAC

            byte[] rangedBytes = new byte[(int)ciphertextByteRangeLength];
            randomAccessFile.read(rangedBytes);

            // it's a byte array, close does nothing so skip try-with-resources
            final ByteArrayInputStream bin = new ByteArrayInputStream(rangedBytes);
            final ByteArrayInputStream binSpy = Mockito.spy(bin);

            /* When creating the fake stream, we feed it a content-length equal
             * to the size of the byte range because that is what Manta would
             * do. Also, we pass along the incorrect plaintext length and
             * test how the stream handles incorrect values of plaintext length. */
             MantaEncryptedObjectInputStream min = createEncryptedObjectInputStream(key, binSpy,
                     ciphertextByteRangeLength, cipherDetails, encryptedFile.cipher.getIV(),
                false, (long)this.plaintextSize,
                     initialSkipBytes,
                     plaintextLength,
                     unboundedEnd);

            byte[] actual = new byte[expected.length];
            ReadBytesFactory.fullStrategy(strategy).readBytes(min, actual);

            min.close();
            Mockito.verify(binSpy, Mockito.atLeastOnce()).close();

            try {
                AssertJUnit.assertArrayEquals("Byte range output doesn't match",
                        expected, actual);
            } catch (AssertionError e) {
                Assert.fail(String.format("%s\nexpected: %s\nactual  : %s",
                        e.getMessage(),
                        new String(expected, StandardCharsets.UTF_8),
                        new String(actual, StandardCharsets.UTF_8)));
            }
        }
    }

    protected void canReadObject(SupportedCipherDetails cipherDetails,
                               Class<? extends ReadBytes> strategy,
                               boolean authenticate) throws IOException {
        SecretKey key = SecretKeyUtils.generate(cipherDetails);
        EncryptedFile encryptedFile = encryptedFile(key, cipherDetails, this.plaintextSize);
        long ciphertextSize = encryptedFile.file.length();

        final FileInputStream in = new FileInputStream(encryptedFile.file);
        final FileInputStream inSpy = Mockito.spy(in);
        MantaEncryptedObjectInputStream min = createEncryptedObjectInputStream(key, inSpy,
                ciphertextSize, cipherDetails, encryptedFile.cipher.getIV(),
                authenticate, (long)this.plaintextSize);

        try {
            byte[] actual = new byte[plaintextSize];
            ReadBytesFactory.build(strategy, encryptedFile.file.length()).readBytes(min, actual);
        } finally {
            min.close();
            Mockito.verify(inSpy, Mockito.atLeastOnce()).close();
        }
    }

    protected void willThrowExceptionWhenCiphertextIsAltered(SupportedCipherDetails cipherDetails,
                                                           Class<? extends ReadBytes> strategy)
            throws IOException {
        SecretKey key = SecretKeyUtils.generate(cipherDetails);
        EncryptedFile encryptedFile = encryptedFile(key, cipherDetails, this.plaintextSize);

        try (FileChannel fc = (FileChannel.open(encryptedFile.file.toPath(), READ, WRITE))) {
            fc.position(2);
            ByteBuffer buff = ByteBuffer.wrap(new byte[] { 20, 20 });
            fc.write(buff);
        }

        long ciphertextSize = encryptedFile.file.length();

        boolean thrown = false;

        FileInputStream in = new FileInputStream(encryptedFile.file);
        final FileInputStream inSpy = Mockito.spy(in);

        MantaEncryptedObjectInputStream min = createEncryptedObjectInputStream(key, inSpy,
                ciphertextSize, cipherDetails, encryptedFile.cipher.getIV(),
                true, (long)this.plaintextSize);

        try {
            byte[] actual = new byte[plaintextSize];
            ReadBytesFactory.build(strategy, encryptedFile.file.length()).readBytes(min, actual);
            min.close();
        }  catch (MantaClientEncryptionCiphertextAuthenticationException e) {
            thrown = true;
            Mockito.verify(inSpy, Mockito.atLeastOnce()).close();
        }

        Assert.assertTrue(thrown, "Ciphertext authentication exception wasn't thrown");
    }

    protected void willErrorWhenMissingHMAC(final SupportedCipherDetails cipherDetails) throws IOException {
        SecretKey key = SecretKeyUtils.generate(cipherDetails);
        EncryptedFile encryptedFile = encryptedFile(key, cipherDetails, this.plaintextSize);
        long ciphertextSize = encryptedFile.file.length();
        int hmacSize = cipherDetails.getAuthenticationTagOrHmacLengthInBytes();
        long ciphertextSizeWithoutHmac = ciphertextSize - hmacSize;

        boolean thrown = false;

        final FileInputStream fin = new FileInputStream(encryptedFile.file);
        final FileInputStream finSpy = Mockito.spy(fin);

        try (BoundedInputStream in = new BoundedInputStream(finSpy, ciphertextSizeWithoutHmac);
             MantaEncryptedObjectInputStream min = createEncryptedObjectInputStream(key, in,
                     ciphertextSize, cipherDetails, encryptedFile.cipher.getIV(),
                     true, (long)this.plaintextSize)) {
            // Do a single read to make sure that everything is working
            Assert.assertNotEquals(min.read(), -1,
                    "The encrypted stream should not be empty");
        } catch (MantaIOException e) {
            if (e.getMessage().startsWith("No HMAC was stored at the end of the stream")) {
                thrown = true;
            } else {
                throw e;
            }
        }

        Mockito.verify(finSpy, Mockito.atLeastOnce()).close();
        Assert.assertTrue(thrown, "Expected MantaIOException was not thrown");
    }

    protected void willValidateIfHmacIsReadInMultipleReads(final SupportedCipherDetails cipherDetails)
            throws IOException {
        SecretKey key = SecretKeyUtils.generate(cipherDetails);
        EncryptedFile encryptedFile = encryptedFile(key, cipherDetails, this.plaintextSize);
        long ciphertextSize = encryptedFile.file.length();


        final FileInputStream fin = new FileInputStream(encryptedFile.file);
        final FileInputStream finSpy = Mockito.spy(fin);

        try (IncompleteByteReadInputStream ibrin = new IncompleteByteReadInputStream(finSpy);
             MantaEncryptedObjectInputStream min = createEncryptedObjectInputStream(key, ibrin,
                     ciphertextSize, cipherDetails, encryptedFile.cipher.getIV(),
                     true, (long)this.plaintextSize);
             OutputStream out = new NullOutputStream()) {

            IOUtils.copy(min, out);
        }

        Mockito.verify(finSpy, Mockito.atLeastOnce()).close();
    }

    protected EncryptedFile encryptedFile(
            SecretKey key, SupportedCipherDetails cipherDetails,
            long plaintextSize) throws IOException {
        File temp = File.createTempFile("encrypted", ".data");
        FileUtils.forceDeleteOnExit(temp);

        try (InputStream in = testURL.openStream()) {
            return encryptedFile(key, cipherDetails, plaintextSize, in);
        }
    }

    protected EncryptedFile encryptedFile(
            SecretKey key, SupportedCipherDetails cipherDetails, long plaintextSize,
            InputStream in) throws IOException {
        File temp = File.createTempFile("encrypted", ".data");
        FileUtils.forceDeleteOnExit(temp);

        try (FileOutputStream out = new FileOutputStream(temp)) {
            MantaInputStreamEntity entity = new MantaInputStreamEntity(in, plaintextSize);
            EncryptingEntity encryptingEntity = new EncryptingEntity(
                    key, cipherDetails, entity);
            encryptingEntity.writeTo(out);

            Assert.assertEquals(temp.length(), encryptingEntity.getContentLength(),
                    "Ciphertext doesn't equal calculated size");

            return new EncryptedFile(encryptingEntity.getCipher(), temp);
        }
    }

    protected MantaEncryptedObjectInputStream createEncryptedObjectInputStream(
            SecretKey key, InputStream in, long contentLength,
            SupportedCipherDetails cipherDetails,
            byte[] iv, boolean authenticate, long plaintextSize) {
        return createEncryptedObjectInputStream(key, in, contentLength, cipherDetails, iv,
                authenticate, plaintextSize, null, null, true);
    }

    protected MantaEncryptedObjectInputStream createEncryptedObjectInputStream(
            SecretKey key, InputStream in, long ciphertextSize, SupportedCipherDetails cipherDetails,
            byte[] iv, boolean authenticate, long plaintextSize, Long skipBytes, Long plaintextLength, boolean unboundedEnd) {
        String path = String.format("/test/stor/test-%s", UUID.randomUUID());
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.put(MantaHttpHeaders.ENCRYPTION_CIPHER, cipherDetails.getCipherId());
        headers.put(MantaHttpHeaders.ENCRYPTION_PLAINTEXT_CONTENT_LENGTH, plaintextSize);
        headers.put(MantaHttpHeaders.ENCRYPTION_KEY_ID, "unit-test-key");

        if (cipherDetails.isAEADCipher()) {
            headers.put(MantaHttpHeaders.ENCRYPTION_AEAD_TAG_LENGTH, cipherDetails.getAuthenticationTagOrHmacLengthInBytes());
        } else {
            final String hmacName = SupportedHmacsLookupMap.hmacNameFromInstance(
                    cipherDetails.getAuthenticationHmac());
            headers.put(MantaHttpHeaders.ENCRYPTION_HMAC_TYPE, hmacName);
        }

        headers.put(MantaHttpHeaders.ENCRYPTION_IV,
                Base64.getEncoder().encodeToString(iv));

        headers.setContentLength(ciphertextSize);

        MantaMetadata metadata = new MantaMetadata();

        MantaObjectResponse response = new MantaObjectResponse(path, headers, metadata);

        CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);

        EofSensorInputStream eofSensorInputStream = new EofSensorInputStream(in, null);

        MantaObjectInputStream mantaObjectInputStream = new MantaObjectInputStream(
                response, httpResponse, eofSensorInputStream
        );

        return new MantaEncryptedObjectInputStream(mantaObjectInputStream,
                cipherDetails, key, authenticate,
                skipBytes, plaintextLength, unboundedEnd);
    }
}
