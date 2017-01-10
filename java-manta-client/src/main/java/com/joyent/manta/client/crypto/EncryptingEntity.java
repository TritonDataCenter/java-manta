/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client.crypto;

import com.joyent.manta.exception.MantaClientEncryptionException;
import com.joyent.manta.exception.MantaIOException;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.SecureRandom;

/**
 * {@link HttpEntity} implementation that wraps an entity and encrypts its
 * output.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class EncryptingEntity implements HttpEntity {
    /**
     * Value for an unknown stream length.
     */
    private static final long UNKNOWN_LENGTH = -1L;

    /**
     * Content transfer encoding to send (set to null for none).
     */
    private static final Header CRYPTO_TRANSFER_ENCODING = null;

    /**
     * Content type to store encrypted content as.
     */
    private static final Header CRYPTO_CONTENT_TYPE = new BasicHeader(
            HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_OCTET_STREAM.toString());

    /**
     * Secret key to encrypt stream with.
     */
    private final SecretKey key;

    /**
     * Attributes of the cipher used for encryption.
     */
    private final SupportedCipherDetails cipherDetails;

    /**
     * Total length of the stream in bytes.
     */
    private long originalLength;

    /**
     * Underlying entity that is being encrypted.
     */
    private final HttpEntity wrapped;

    /**
     * Cipher implementation used to encrypt as a stream.
     */
    private Cipher cipher;

    /**
     * Source of entropy for the encryption algorithm.
     */
    private SecureRandom random;

    /**
     * Creates a new instance with an known stream size.
     *
     * @param key key to encrypt stream with
     * @param cipherDetails cipher to encrypt stream with
     * @param wrapped underlying stream to encrypt
     * @param random source of entropy for the encryption algorithm
     */
    public EncryptingEntity(final SecretKey key,
                            final SupportedCipherDetails cipherDetails,
                            final HttpEntity wrapped,
                            final SecureRandom random) {
        if (originalLength > cipherDetails.getMaximumPlaintextSizeInBytes()) {
            String msg = String.format("Input content length exceeded maximum "
            + "[%d] number of bytes supported by cipher [%s]",
                    cipherDetails.getMaximumPlaintextSizeInBytes(),
                    cipherDetails.getCipherAlgorithm());
            throw new MantaClientEncryptionException(msg);
        }

        this.key = key;
        this.cipherDetails = cipherDetails;
        this.originalLength = wrapped.getContentLength();
        this.wrapped = wrapped;
        this.random = random;
    }

    @Override
    public boolean isRepeatable() {
        return wrapped.isRepeatable();
    }

    @Override
    public boolean isChunked() {
        return originalLength < 0;
    }

    @Override
    public long getContentLength() {
        if (originalLength >= 0) {
            return cipherDetails.cipherTextSize(originalLength);
        } else {
            return UNKNOWN_LENGTH;
        }
    }

    public long getOriginalLength() {
        return originalLength;
    }

    @Override
    public Header getContentType() {
        return CRYPTO_CONTENT_TYPE;
    }

    @Override
    public Header getContentEncoding() {
        return CRYPTO_TRANSFER_ENCODING;
    }

    @Override
    public InputStream getContent() throws IOException, UnsupportedOperationException {
        return wrapped.getContent();
    }

    @Override
    public void writeTo(final OutputStream out) throws IOException {
        cipher = cipherDetails.getCipher();

        try {
            byte[] iv = new byte[cipherDetails.getIVLengthInBytes()];
            random.nextBytes(iv);

            cipher.init(Cipher.ENCRYPT_MODE, this.key, cipherDetails.getEncryptionParameterSpec(iv));
        } catch (InvalidKeyException e) {
            MantaClientEncryptionException mcee = new MantaClientEncryptionException(
                    "There was a problem loading private key", e);
            String details = String.format("key=%s, algorithm=%s",
                    key.getAlgorithm(), key.getFormat());
            mcee.setContextValue("key_details", details);
            throw mcee;
        } catch (InvalidAlgorithmParameterException e) {
            throw new MantaClientEncryptionException(
                    "There was a problem with the passed algorithm parameters", e);
        }

        try (CipherOutputStream cout = new CipherOutputStream(out, cipher)) {
            long bytesCopied = IOUtils.copyLarge(getContent(), cout);
            cout.flush();

            /* If we don't know the length of the underlying content stream, we
             * count the number of bytes written, so that it is available. */
            if (originalLength == UNKNOWN_LENGTH) {
                originalLength = bytesCopied;
            } else if (originalLength != bytesCopied) {
                MantaIOException e = new MantaIOException("Bytes copied doesn't equal the "
                        + "specified content length");
                e.setContextValue("specifiedContentLength", originalLength);
                e.setContextValue("actualContentLength", bytesCopied);
                throw e;
            }
        }
    }

    @Override
    public boolean isStreaming() {
        return wrapped.isStreaming();
    }

    @SuppressWarnings("deprecation")
    @Override
    public void consumeContent() throws IOException {
        this.wrapped.consumeContent();
    }

    public Cipher getCipher() {
        return cipher;
    }
}
