/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client.crypto;

import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.exception.MantaClientEncryptionCiphertextAuthenticationException;
import com.joyent.manta.exception.MantaClientEncryptionException;
import com.joyent.manta.exception.MantaIOException;
import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.io.input.CloseShieldInputStream;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.exception.ContextedException;
import org.apache.commons.lang3.exception.ExceptionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.util.Arrays;
import java.util.Base64;
import java.util.function.Supplier;

/**
 * An {@link InputStream} implementation that decrypts client-side encrypted
 * Manta objects as a stream and performs authentication when the end of the
 * stream is reached.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
@SuppressWarnings("Duplicates")
public class MantaEncryptedObjectInputStream extends MantaObjectInputStream {
    private static final long serialVersionUID = 8536248985759134599L;

    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(MantaEncryptedObjectInputStream.class);

    /**
     * Default buffer size to use when reading chunks of data from streams.
     */
    private static final int DEFAULT_BUFFER_SIZE = 8192;

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
    private final Mac hmac;

    /**
     * The cipher stream that wraps the backing stream allowing for streaming decryption.
     */
    private final CipherInputStream cipherInputStream;

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
     * Creates a new instance that decrypts the backing stream with the specified key.
     *
     * @param backingStream stream to read data from
     * @param secretKey secret key used to decrypt
     */
    public MantaEncryptedObjectInputStream(final MantaObjectInputStream backingStream,
                                           final SecretKey secretKey) {
        super(backingStream);

        this.cipherDetails = findCipherDetails();
        this.cipher = cipherDetails.getCipher();
        this.secretKey = secretKey;
        this.hmac = findHmac();
        initializeCipher();
        initializeHmac();
        this.cipherInputStream = createCryptoStream();
    }

    /**
     * Initializes the HMAC with the secret key and the  IV for the
     * encrypted object.
     */
    private void initializeHmac() {
        if (this.hmac == null) {
            return;
        }

        try {
            this.hmac.init(secretKey);
        } catch (InvalidKeyException e) {
            String msg = "Couldn't initialize HMAC with secret key";
            MantaClientEncryptionException mce = new MantaClientEncryptionException(msg, e);
            annotateException(mce);
            throw mce;
        }

        this.hmac.update(this.cipher.getIV());
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
    private CipherInputStream createCryptoStream() {
        // No need to calculate HMAC because we are using a AEAD cipher
        if (this.cipherDetails.isAEADCipher()) {
            return new CipherInputStream(super.getBackingStream(), this.cipher);
        }

        final long hmacSize = this.hmac.getMacLength();
        final long adjustedContentLength = super.getContentLength() - hmacSize;
        CloseShieldInputStream closeShieldInputStream = new CloseShieldInputStream(super.getBackingStream());
        BoundedInputStream bin = new BoundedInputStream(closeShieldInputStream, adjustedContentLength);
        return new CipherInputStream(bin, this.cipher);
    }

    /**
     * Finds the cipher used to encrypt the object that we are streaming.
     *
     * @return cipher details instance
     * @throws MantaClientEncryptionException when the cipher specified is invalid
     */
    private SupportedCipherDetails findCipherDetails() {
        final String cipherId = getHeaderAsString(MantaHttpHeaders.ENCRYPTION_CIPHER);

        if (cipherId == null || cipherId.isEmpty()) {
            String msg = "Cipher id / cipher type must not be null nor empty";
            MantaClientEncryptionException e = new MantaClientEncryptionException(msg);
            annotateException(e);
            throw e;
        }

        SupportedCipherDetails details = SupportedCiphersLookupMap.INSTANCE.getWithCaseInsensitiveKey(cipherId);

        if (details == null) {
            String msg = "Unsupported cipher id / cipher type "
                    + "used to encrypt object";
            MantaClientEncryptionException e = new MantaClientEncryptionException(msg);
            annotateException(e);
            throw e;
        }

        return details;
    }

    /**
     * Finds the correct HMAC instance based on the metadata supplied from the
     * encrypted object.
     *
     * @return HMAC instance or null when encrypted by a AEAD cipher
     */
    private Mac findHmac() {
        if (this.cipherDetails.isAEADCipher()) {
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

        Supplier<Mac> macSupplier = SupportedHmacsLookupMap.INSTANCE.get(hmacString);

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
    public int read() throws IOException {
        if (this.closed) {
            MantaIOException e = new MantaIOException("Can't read a closed stream");
            e.setContextValue("path", getPath());
            throw e;
        }

        try {
            final int b = cipherInputStream.read();

            if (hmac != null && b != -1) {
                hmac.update((byte) b);
            }

            if (b > 0) {
                plaintextBytesRead++;
            }

            return b;
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
        if (this.closed) {
            MantaIOException e = new MantaIOException("Can't read a closed stream");
            e.setContextValue("path", getPath());
            throw e;
        }

        try {
            final int read = cipherInputStream.read(bytes);

            if (hmac != null && read > 0) {
                hmac.update(bytes, 0, read);
            }

            if (read > 0) {
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
    public int read(final byte[] bytes, final int off, final int len) throws IOException {
        if (this.closed) {
            MantaIOException e = new MantaIOException("Can't read a closed stream");
            e.setContextValue("path", getPath());
            throw e;
        }

        try {
            final int read = cipherInputStream.read(bytes, off, len);

            if (hmac != null && read > 0) {
                hmac.update(bytes, off, read);
            }

            if (read > 0) {
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

        if (numberOfBytesToSkip <= 0) {
            return 0;
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
        int skippedInLastRead;

        do {
            skippedInLastRead = read(buf);
            skipped += skippedInLastRead;
        } while (skippedInLastRead > -1 || skipped < numberOfBytesToSkip);

        return skipped;
    }

    @Override
    public int available() throws IOException {
        if (this.closed) {
            MantaIOException e = new MantaIOException("Can't calculate available on a closed stream");
            e.setContextValue("path", getPath());
            throw e;
        }

        return cipherInputStream.available();
    }

    /**
     * Reads all of the remaining bytes in a stream and calculates the HMAC for them.
     * This method is called when closing the stream so that we can close the stream
     * and verify the HMAC or AEAD tag.
     * @throws IOException thrown when there is a problem reading the cipher text stream
     */
    private void readRemainingBytes() throws IOException {
        if (cipherInputStream.available() <= 0) {
            return;
        }

        final int bufferSize = calculateBufferSize();
        byte[] buf = new byte[bufferSize];
        int read;

        while ((read = read(buf)) >= 0) {
            if (hmac != null) {
                hmac.update(buf, 0, read);
            }
        }
    }

    /**
     * Calculates the size of buffer to use when reading chunks of bytes based on the known
     * content length.
     * @return size of buffer to read into memory
     */
    private int calculateBufferSize() {
        final long contentLength = ObjectUtils.firstNonNull(getContentLength(), -1L);
        long cipherTextContentLength = contentLength;
        if (this.cipherDetails.isAEADCipher()) {
            cipherTextContentLength -= this.cipherDetails.getAuthenticationTagOrHmacLengthInBytes();
        } else {
            cipherTextContentLength -= this.hmac.getMacLength();
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

            readRemainingBytes();
            this.closed = true;
        }

        IOUtils.closeQuietly(cipherInputStream);

        if (hmac != null) {
            byte[] checksum = hmac.doFinal();
            byte[] expected = new byte[this.hmac.getMacLength()];
            int readHmacBytes = super.getBackingStream().read(expected);

            if (readHmacBytes != this.hmac.getMacLength()) {
                MantaIOException e = new MantaIOException("The HMAC stored was in the incorrect size");
                annotateException(e);
                e.setContextValue("expectedHmacSize", this.hmac.getMacLength());
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
                String msg = "Stored authentication HMAC failed to validate ciphertext.";
                MantaClientEncryptionCiphertextAuthenticationException e =
                        new MantaClientEncryptionCiphertextAuthenticationException(msg);
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
        exception.setContextValue("cipherId", getHeaderAsString(MantaHttpHeaders.ENCRYPTION_CIPHER));
        exception.setContextValue("cipherDetails", this.cipherDetails);

        if (this.hmac != null) {
            exception.setContextValue("hmac", this.hmac.getAlgorithm());
        } else {
            exception.setContextValue("hmac", "null");
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

        return this.cipherDetails.plaintextSize(super.getContentLength());
    }
}
