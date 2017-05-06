/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import com.joyent.manta.client.ClosedByteRange;
import com.joyent.manta.client.ClosedRange;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.client.NullByteRange;
import com.joyent.manta.client.NullRange;
import com.joyent.manta.client.OpenByteRange;
import com.joyent.manta.client.OpenRange;
import com.joyent.manta.client.Range;
import com.joyent.manta.exception.MantaClientEncryptionCiphertextAuthenticationException;
import com.joyent.manta.exception.MantaClientEncryptionException;
import com.joyent.manta.exception.MantaIOException;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.util.NotThreadSafe;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.exception.ContextedException;
import org.apache.commons.lang3.exception.ExceptionContext;
import org.bouncycastle.crypto.macs.HMac;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.jcajce.io.CipherInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;

import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * <p>An {@link InputStream} implementation that decrypts client-side encrypted
 * Manta objects as a stream and performs authentication when the end of the
 * stream is reached.</p>
 *
 * <p><strong>This class is not thread-safe.</strong></p>
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
@SuppressWarnings("Duplicates")
@NotThreadSafe
public class MantaEncryptedObjectInputStream extends MantaObjectInputStream {
    private static final long serialVersionUID = 8536248985759134599L;

    /**
     * End of file marker for streams.
     */
    private static final int EOF = -1;

    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(MantaEncryptedObjectInputStream.class);

    /**
     * Default buffer size to use when reading chunks of data from streams.
     */
    private static final int DEFAULT_BUFFER_SIZE = 512;

    /**
     * The cipher and its settings used to decrypt the backing stream.
     */
    private final SupportedCipherDetails cipherDetails;

    /**
     * The secret key instance used with the cipher to decrypt the backing stream.
     */
    private final SecretKey secretKey;

    /**
     * The cipher instance used to decrypt the backing stream.
     */
    private final Cipher cipher;

    /**
     * The HMAC instance (if using non AEAD encryption) used to authenticate the ciphertext.
     */
    private final HMac hmac;

    /**
     * The stream that wraps the backing stream allowing for streaming decryption.
     * Somewhere chained in this stream is a {@link CipherInputStream}.
     */
    private final InputStream cipherInputStream;

    /**
     * The total number of plaintext bytes read.
     */
    private long plaintextBytesRead = 0L;

    /**
     * Flag indicating that the underlying stream is closed.
     */
    private volatile boolean closed = false;

    /**
     * Lock object used to synchronize close calls.
     */
    private final Object closeLock = new Object();

    /**
     * Flag indicating if we perform authentication on the ciphertext.
     */
    private final boolean authenticateCiphertext;

    /**
     * Length of ciphertext and possible HMAC signature in bytes.
     */
    private final Long contentLength;

    /**
     * Encryted byte range for ciphertext input stream.
     */
    private final EncryptedByteRange<? extends NullRange,
                                     ? extends OpenRange,
                                     ? extends ClosedRange,
                                     ? extends Range> range;

    /**
     * The number of bytes to skip on the first read/skip operation.
     */
    private long initialBytesToSkip;

    /**
     * Creates a new instance that decrypts the backing stream with the specified key.
     *
     * @param backingStream stream to read data from
     * @param cipherDetails cipher/mode properties definition object
     * @param secretKey secret key used to decrypt
     * @param authenticateCiphertext when true we perform authentication on the ciphertext
     */
    public MantaEncryptedObjectInputStream(final MantaObjectInputStream backingStream,
                                           final SupportedCipherDetails cipherDetails,
                                           final SecretKey secretKey,
                                           final boolean authenticateCiphertext) {
        this(backingStream, cipherDetails, secretKey, authenticateCiphertext,
                new EncryptedNullByteRange<NullByteRange,
                                           OpenByteRange,
                                           ClosedByteRange>(new NullByteRange(), cipherDetails));
    }

    /**
     * Creates a new instance that decrypts the backing stream with the
     * specified key.
     *
     * @param backingStream stream to read data from
     * @param cipherDetails cipher/mode properties definition object
     * @param secretKey secret key used to decrypt
     * @param authenticateCiphertext when true we perform authentication on the ciphertext value is
     *            ignored when operating with a AEAD cipher mode
     * @param range encrypted range for the input stream
     *
     * @throws InvalidArgumentException if either {@code cipherDetails}, {@code secretKey}, or
     *                 {@code range} is null
     */
    public MantaEncryptedObjectInputStream(final MantaObjectInputStream backingStream,
                                           final SupportedCipherDetails cipherDetails,
                                           final SecretKey secretKey,
                                           final boolean authenticateCiphertext,
                                           final EncryptedByteRange<? extends NullRange,
                                                                    ? extends OpenRange,
                                                                    ? extends ClosedRange,
                                                                    ? extends Range> range) {
        super(backingStream);

        assert (cipherDetails != null);
        assert (secretKey != null);
        assert (range != null);

        if (cipherDetails == null) {

            final String msg = "cipherDetails must not be null";
            throw new IllegalArgumentException(msg);
        }

        if (secretKey == null) {

            final String msg = "secretKey must not be null";
            throw new IllegalArgumentException(msg);
        }

        if (range == null) {

            final String msg = "range must not be null";
            throw new IllegalArgumentException(msg);
        }

        this.authenticateCiphertext = authenticateCiphertext;
        this.cipherDetails = cipherDetails;
        this.contentLength = super.getContentLength();

        this.range = range;

        this.cipher = cipherDetails.getCipher();
        this.secretKey = secretKey;
        this.hmac = findHmac();

        this.initialBytesToSkip = range.getOffset();
        initializeCipher();
        this.cipherInputStream = createCryptoStream();
        initializeHmac();
    }

    /**
     * Initializes the HMAC with the secret key and the  IV for the
     * encrypted object.
     */
    private void initializeHmac() {
        if (this.hmac == null) {
            return;
        }

        this.hmac.init(new KeyParameter(secretKey.getEncoded()));

        final byte[] iv = this.cipher.getIV();
        this.hmac.update(iv, 0, iv.length);
    }

    /**
     * Initializes the cipher with the object's IV.
     */
    private void initializeCipher() {
        String ivString = getHeaderAsString(MantaHttpHeaders.ENCRYPTION_IV);

        if (ivString == null || ivString.isEmpty()) {
            String msg = "Initialization Vector (IV) was not set for the object. Unable to decrypt.";
            MantaClientEncryptionException e = new MantaClientEncryptionException(msg);
            annotateException(e);
            throw e;
        }

        byte[] iv = Base64.getDecoder().decode(ivString);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("IV: {}", Hex.encodeHexString(iv));
        }

        final int mode = Cipher.DECRYPT_MODE;

        try {
            this.cipher.init(mode, secretKey, cipherDetails.getEncryptionParameterSpec(iv));

            if (range instanceof OpenRange) {

                OpenRange openRange = (OpenRange) range;

                cipherDetails.updateCipherToPosition(this.cipher, openRange.getStart());

            } else if (range instanceof ClosedRange) {

                ClosedRange closedRange = (ClosedRange) range;

                cipherDetails.updateCipherToPosition(this.cipher, closedRange.getStart());
            }

        } catch (InvalidKeyException | InvalidAlgorithmParameterException e) {
            String msg = "Error initializing cipher";
            MantaClientEncryptionException mce = new MantaClientEncryptionException(msg, e);
            annotateException(mce);
            throw mce;
        }
    }

    /**
     * Creates a new instance of a {@link CipherInputStream} based on the parameters
     * returned as HTTP headers for the object.
     *
     * @return a configured decrypting stream
     */
    private InputStream createCryptoStream() {

        InputStream source = super.getBackingStream();

        // Need to calculate MAC because we are not using an AEAD cipher
        if (!cipherDetails.isAEADCipher()) {

            long macBytes = 0;

            if (range.includesMac()) {

                // returned bytes will have an mac
                macBytes = cipherDetails.getAuthenticationTagOrHmacLengthInBytes();

            }

            BoundedInputStream bin = new BoundedInputStream(source, contentLength - macBytes);

            bin.setPropagateClose(false);
            source = bin;
        }

        final CipherInputStream cin = new CipherInputStream(source, cipher);

        final Range unEncryptedRange = range.doUndecorate();
        final Optional<Long> length = unEncryptedRange.getLength();

        if (length.isPresent()) {

            final long l = length.get();
            final long bo = range.getOffset();

            return new BoundedInputStream(cin, l + bo);
        }

        return cin;
    }

    /**
     * Finds the correct HMAC instance based on the metadata supplied from the
     * encrypted object.
     *
     * @return HMAC instance or null when encrypted by a AEAD cipher
     */
    private HMac findHmac() {
        if (this.cipherDetails.isAEADCipher() || !this.authenticateCiphertext) {
            return null;
        }

        String hmacString = getHeaderAsString(MantaHttpHeaders.ENCRYPTION_HMAC_TYPE);

        if (hmacString == null) {
            String msg = String.format("HMAC header metadata [%s] was missing from object",
                    MantaHttpHeaders.ENCRYPTION_HMAC_TYPE);
            MantaClientEncryptionException e = new MantaClientEncryptionException(msg);
            annotateException(e);
            throw e;
        }

        if (hmacString.isEmpty()) {
            String msg = String.format("HMAC header metadata [%s] was empty on object",
                    MantaHttpHeaders.ENCRYPTION_HMAC_TYPE);
            MantaClientEncryptionException e = new MantaClientEncryptionException(msg);
            annotateException(e);
            throw e;
        }

        Supplier<HMac> macSupplier = SupportedHmacsLookupMap.INSTANCE.get(hmacString);

        if (macSupplier == null) {
            String msg = String.format("HMAC stored in header metadata [%s] is unsupported",
                    hmacString);
            MantaClientEncryptionException e = new MantaClientEncryptionException(msg);
            annotateException(e);
            throw e;
        }

        return macSupplier.get();
    }

    @Override
    public InputStream getBackingStream() {
        return this.cipherInputStream;
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void mark(final int readlimit) {
        throw new UnsupportedOperationException("mark is not a supported operation on " + getClass());
    }

    @Override
    public void reset() throws IOException {
        throw new UnsupportedOperationException("reset is not a supported operation on " + getClass());
    }

    @Override
    public int read() throws IOException {
        if (this.closed) {
            MantaIOException e = new MantaIOException("Can't read a closed stream");
            annotateException(e);
            throw e;
        }

        skipInitialBytes();

        try {
            final int read = cipherInputStream.read();

            if (hmac != null && read > EOF && authenticateCiphertext) {
                hmac.update((byte) read);
            }

            if (read > EOF) {
                plaintextBytesRead++;
            }

            return read;
        } catch (IOException e) {
            final Throwable cause = e.getCause();
            if (cause != null && cause.getClass().equals(AEADBadTagException.class)) {
                MantaClientEncryptionException mce = new MantaClientEncryptionCiphertextAuthenticationException(cause);
                annotateException(mce);
                throw mce;
            } else {
                throw e;
            }
        }
    }

    @Override
    public int read(final byte[] bytes) throws IOException {
        return read(bytes, true);
    }

    /**
     * Reads some number of bytes from the input stream and stores them into
     * the buffer array <code>b</code>. The number of bytes actually read is
     * returned as an integer.  This method blocks until input data is
     * available, end of file is detected, or an exception is thrown.
     *
     * <p> If the length of <code>b</code> is zero, then no bytes are read and
     * <code>0</code> is returned; otherwise, there is an attempt to read at
     * least one byte. If no byte is available because the stream is at the
     * end of the file, the value <code>-1</code> is returned; otherwise, at
     * least one byte is read and stored into <code>b</code>.
     *
     * <p> The first byte read is stored into element <code>b[0]</code>, the
     * next one into <code>b[1]</code>, and so on. The number of bytes read is,
     * at most, equal to the length of <code>b</code>. Let <i>k</i> be the
     * number of bytes actually read; these bytes will be stored in elements
     * <code>b[0]</code> through <code>b[</code><i>k</i><code>-1]</code>,
     * leaving elements <code>b[</code><i>k</i><code>]</code> through
     * <code>b[b.length-1]</code> unaffected.
     *
     * <p> The <code>read(b)</code> method for class <code>InputStream</code>
     * has the same effect as: <pre><code> read(b, 0, b.length) </code></pre>
     *
     * @param      bytes   the buffer into which the data is read.
     * @param      checkIfClosed when true an exception is thrown if close()
     *                           has been called
     * @return     the total number of bytes read into the buffer, or
     *             <code>-1</code> if there is no more data because the end of
     *             the stream has been reached.
     * @exception  IOException  If the first byte cannot be read for any reason
     * other than the end of the file, if the input stream has been closed, or
     * if some other I/O error occurs.
     * @exception  NullPointerException  if <code>b</code> is <code>null</code>.
     * @see        java.io.InputStream#read(byte[], int, int)
     */
    private int read(final byte[] bytes, final boolean checkIfClosed) throws IOException {
        if (this.closed && checkIfClosed) {
            MantaIOException e = new MantaIOException("Can't read a closed stream");
            e.setContextValue("path", getPath());
            throw e;
        }

        skipInitialBytes();

        final int read;

        try {
            read = cipherInputStream.read(bytes);
        } catch (IOException e) {
            final Throwable cause = e.getCause();
            if (cause != null && cause.getClass().equals(AEADBadTagException.class)) {
                MantaClientEncryptionException mce = new MantaClientEncryptionCiphertextAuthenticationException(cause);
                annotateException(mce);
                throw mce;
            } else {
                throw e;
            }
        }

        if (hmac != null && read > EOF && authenticateCiphertext) {
            hmac.update(bytes, 0, read);
        }

        if (read > EOF) {
            plaintextBytesRead += read;
        }

        return read;
    }

    @Override
    public int read(final byte[] bytes, final int off, final int len) throws IOException {
        if (this.closed) {
            MantaIOException e = new MantaIOException("Can't read a closed stream");
            e.setContextValue("path", getPath());
            throw e;
        }

        skipInitialBytes();

        try {
            final int read = cipherInputStream.read(bytes, off, len);

            if (hmac != null && read > EOF && authenticateCiphertext) {
                hmac.update(bytes, off, read);
            }

            if (read > EOF) {
                plaintextBytesRead += read;
            }

            return read;
        } catch (IOException e) {
            final Throwable cause = e.getCause();
            if (cause != null && cause.getClass().equals(AEADBadTagException.class)) {
                MantaClientEncryptionException mce = new MantaClientEncryptionCiphertextAuthenticationException(cause);
                annotateException(mce);
                throw mce;
            } else {
                throw e;
            }
        }
    }

    @Override
    public long skip(final long numberOfBytesToSkip) throws IOException {
        if (this.closed) {
            MantaIOException e = new MantaIOException("Can't skip a closed stream");
            e.setContextValue("path", getPath());
            throw e;
        }

        skipInitialBytes();

        if (numberOfBytesToSkip <= 0) {
            return 0;
        }

        /* When using a CipherInputStream with some algorithms, the skip() method
         * is horribly broken and doesn't return the correct number of bytes
         * skipped. In order to accurately report the number of bytes skipped
         * we use a read() method call, throw away the data and count the
         * number of successful reads. */
        if (!authenticateCiphertext || cipherDetails.isAEADCipher()) {
            long skipped = 0L;

            for (long l = 0L; l < numberOfBytesToSkip; l++) {
                final int read = cipherInputStream.read();

                if (read > EOF) {
                    skipped++;
                }
            }

            return skipped;
        }

        final int defaultBufferSize = calculateBufferSize();
        final int bufferSize;

        if (numberOfBytesToSkip < defaultBufferSize) {
            bufferSize = (int)numberOfBytesToSkip;
        } else {
            bufferSize = defaultBufferSize;
        }

        byte[] buf = new byte[bufferSize];

        long skipped = 0;
        int skippedInLastRead = 0;

        while (skippedInLastRead > EOF && skipped <= numberOfBytesToSkip) {
            skippedInLastRead = read(buf);

            if (skippedInLastRead > EOF) {
                skipped += skippedInLastRead;
            }
        }

        return skipped;
    }

    @Override
    public int available() throws IOException {
        if (this.closed) {
            MantaIOException e = new MantaIOException("Can't calculate available on a closed stream");
            e.setContextValue("path", getPath());
            throw e;
        }

        skipInitialBytes();

        return cipherInputStream.available();
    }

    /**
     * Skips the initial bytes set to move forward in the plaintext stream.
     * @throws IOException when bytes can't be read from the underlying stream
     */
    private void skipInitialBytes() throws IOException {
        /* We don't use the CipherInputStream.skip() method because it is
         * unreliable across implementations and won't always skip forward
         * the way we expect. Through testing, we've found this is the most
         * reliable way of moving the plaintext forward. */
        while (initialBytesToSkip > 0) {
            int read = cipherInputStream.read();

            if (read > EOF) {
                initialBytesToSkip--;
            } else {
                // We hit the end of the stream, initialBytesToSkip was incorrect, set to 0 and return
                initialBytesToSkip = 0;
                return;
            }
        }
    }

    /**
     * Reads all of the remaining bytes in a stream and calculates the HMAC for them.
     * This method is called when closing the stream so that we can close the stream
     * and verify the HMAC or AEAD tag.
     * @throws IOException thrown when there is a problem reading the cipher text stream
     */
    @SuppressWarnings("EmptyStatement")
    private void readRemainingBytes() throws IOException {
        if (cipherInputStream.available() <= 0) {
            return;
        }

        final int bufferSize = calculateBufferSize();
        byte[] buf = new byte[bufferSize];

        while (read(buf, false) > EOF);
    }

    /**
     * Calculates the size of buffer to use when reading chunks of bytes based on the known
     * content length.
     * @return size of buffer to read into memory
     */
    private int calculateBufferSize() {
        long cipherTextContentLength = ObjectUtils.firstNonNull(getContentLength(), -1L);

        if (cipherTextContentLength >= 0) {
            cipherTextContentLength -= this.cipherDetails.getAuthenticationTagOrHmacLengthInBytes();
        }

        final int bufferSize;

        if (cipherTextContentLength > DEFAULT_BUFFER_SIZE || cipherTextContentLength < 0) {
            bufferSize = DEFAULT_BUFFER_SIZE;
        } else {
            bufferSize = (int)cipherTextContentLength;
        }

        return bufferSize;
    }

    @Override
    public void close() throws IOException {
        synchronized (this.closeLock) {
            if (this.closed) {
                return;
            }

            this.closed = true;
        }

        readRemainingBytes();

        IOUtils.closeQuietly(cipherInputStream);

        if (hmac != null && authenticateCiphertext) {
            byte[] checksum = new byte[hmac.getMacSize()];
            hmac.doFinal(checksum, 0);
            byte[] expected = new byte[this.hmac.getMacSize()];
            int readHmacBytes = super.getBackingStream().read(expected);

            if (super.getBackingStream().read() != EOF) {
                String msg = "Expecting the end of the stream. However, more "
                        + "bytes were available.";
                MantaIOException e = new MantaIOException(msg);
                annotateException(e);
                throw e;
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Calculated HMAC is: {}", Hex.encodeHexString(checksum));
            }

            if (readHmacBytes != this.hmac.getMacSize()) {
                MantaIOException e = new MantaIOException("The HMAC stored was in the incorrect size");
                annotateException(e);
                e.setContextValue("expectedHmacSize", this.hmac.getMacSize());
                e.setContextValue("actualHmacSize", readHmacBytes);
                throw e;
            }

            if (super.getBackingStream().read() >= 0) {
                MantaIOException e = new MantaIOException("More bytes were available than the "
                        + "expected HMAC length");
                annotateException(e);
                throw e;
            }

            super.close();

            if (!Arrays.equals(expected, checksum)) {
                MantaClientEncryptionCiphertextAuthenticationException e =
                        new MantaClientEncryptionCiphertextAuthenticationException();
                annotateException(e);
                e.setContextValue("expected", Hex.encodeHexString(expected));
                e.setContextValue("checksum", Hex.encodeHexString(checksum));
                throw e;
            }
        }
    }

    /**
     * Annotates a {@link ContextedException} with the details of this class in order to aid
     * in debugging.
     *
     * @param exception exception to annotate
     */
    private void annotateException(final ExceptionContext exception) {
        exception.setContextValue("path", getPath());
        exception.setContextValue("etag", this.getEtag());
        exception.setContextValue("lastModified", this.getLastModifiedTime());
        exception.setContextValue("ciphertextContentLength", super.getContentLength());
        exception.setContextValue("plaintextContentLength", this.getContentLength());
        exception.setContextValue("plaintextBytesRead", this.plaintextBytesRead);
        exception.setContextValue("cipherId", getHeaderAsString(MantaHttpHeaders.ENCRYPTION_CIPHER));
        exception.setContextValue("cipherDetails", this.cipherDetails);
        exception.setContextValue("cipherInputStream", this.cipherInputStream);
        exception.setContextValue("authenticationEnabled", this.authenticateCiphertext);
        exception.setContextValue("threadName", Thread.currentThread().getName());

        if (this.cipher != null && this.cipher.getIV() != null) {
            exception.setContextValue("iv", Hex.encodeHexString(this.cipher.getIV()));
        }

        if (this.hmac != null) {
            exception.setContextValue("hmacAlgorithm", this.hmac.getAlgorithmName());
        } else {
            exception.setContextValue("hmacAlgorithm", "null");
        }
    }

    @Override
    public String getContentType() {
        return getMetadata().getOrDefault("e-content-length", super.getContentType());
    }

    /**
     * {@inheritDoc}
     *
     * <p>Unlike the typical implementation, in some cases this method may return an inaccurate
     * size because the plaintext size is not stored in metadata, the stream isn't finished
     * reading nor is the calculation of the plaintext size from ciphertext size accurate.</p>
     *
     * @return a potentially inaccurate content length
     */
    @Override
    public Long getContentLength() {
        String plaintextLengthString = getHeaderAsString(MantaHttpHeaders.ENCRYPTION_PLAINTEXT_CONTENT_LENGTH);

        if (plaintextLengthString != null) {
            return Long.parseLong(plaintextLengthString);
        }

        if (this.closed) {
            return this.plaintextBytesRead;
        }

        // If the plaintext size isn't stored we attempt to compute it, but it may be inaccurate
        if (LOGGER.isInfoEnabled() && this.cipherDetails.plaintextSizeCalculationIsAnEstimate()) {
            LOGGER.info("Plaintext size reported may be inaccurate for object: {}", getPath());
        }

        Long plaintextSize = super.getContentLength();
        Validate.notNull("Content-length header wasn't set by server");
        return this.cipherDetails.plaintextSize(plaintextSize);
    }
}
