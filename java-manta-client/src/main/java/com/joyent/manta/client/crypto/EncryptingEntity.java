/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client.crypto;

import com.joyent.manta.exception.MantaClientEncryptionException;
import com.joyent.manta.exception.MantaIOException;
import com.joyent.manta.http.entity.EmbeddedHttpContent;
import com.joyent.manta.util.HmacOutputStream;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.lang3.Validate;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.Mac;
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
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(EncryptingEntity.class);

    /**
     * Value for an unknown stream length.
     */
    public static final long UNKNOWN_LENGTH = -1L;

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
    private final Cipher cipher;

    /**
     * Source of entropy for the encryption algorithm.
     */
    private final SecureRandom random;

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

        @SuppressWarnings("MagicNumber")
        final int keyBits = key.getEncoded().length << 3;

        if (keyBits != cipherDetails.getKeyLengthBits()) {
            String msg = "Mismatch between algorithm definition and secret key size";
            MantaClientEncryptionException e = new MantaClientEncryptionException(msg);
            e.setContextValue("cipherDetails", cipherDetails.toString());
            e.setContextValue("secretKeyAlgorithm", key.getAlgorithm());
            e.setContextValue("secretKeySizeInBits", String.valueOf(keyBits));
            e.setContextValue("expectedKeySizeInBits", cipherDetails.getKeyLengthBits());
            throw e;
        }

        this.key = key;
        this.cipherDetails = cipherDetails;
        this.originalLength = wrapped.getContentLength();
        this.wrapped = wrapped;
        this.random = random;
        this.cipher = cipherDetails.getCipher();
        initializeCipher();
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
            return cipherDetails.ciphertextSize(originalLength);
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
    public void writeTo(final OutputStream httpOut) throws IOException {
        /* We have to use a "close shield" here because when close() is called
         * on a CipherOutputStream() for two reasons:
         *
         * 1. CipherOutputStream.close() writes additional bytes that a HMAC
         *    would need to read.
         * 2. Since we are going to append a HMAC to the end of the OutputStream
         *    httpOut, then we have to pretend to close it so that the HMAC bytes
         *    are not being written in the middle of the CipherOutputStream and
         *    thereby corrupting the ciphertext. */

        final CloseShieldOutputStream noCloseOut = new CloseShieldOutputStream(httpOut);
        final CipherOutputStream cipherOut = new CipherOutputStream(noCloseOut, cipher);
        final OutputStream out;
        final Mac hmac;

        // Things are a lot more simple if we are using AEAD
        if (cipherDetails.isAEADCipher()) {
            out = cipherOut;
            hmac = null;
        } else {
            hmac = cipherDetails.getAuthenticationHmac();
            try {
                hmac.init(key);
                /* The first bytes of the HMAC are the IV. This is done in order to
                 * prevent IV collision or spoofing attacks. */
                hmac.update(cipher.getIV());
            } catch (InvalidKeyException e) {
                String msg = "Error initializing HMAC with secret key";
                throw new MantaClientEncryptionException(msg, e);
            }
            out = new HmacOutputStream(hmac, cipherOut);
        }

        try {
            copyContentToOutputStream(out);
            /* We don't close quietly because we want the operation to fail if
             * there is an error closing out the CipherOutputStream. */
            out.close();

            if (hmac != null) {
                byte[] hmacBytes = hmac.doFinal();
                Validate.isTrue(hmacBytes.length == cipherDetails.getAuthenticationTagOrHmacLengthInBytes(),
                        "HMAC actual bytes doesn't equal the number of bytes expected");

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("HMAC: {}", Hex.encodeHexString(hmacBytes));
                }

                httpOut.write(hmacBytes);
            }
        } finally {
            IOUtils.closeQuietly(httpOut);
        }
    }

    /**
     * Copies the entity content to the specified output stream and validates
     * that the number of bytes copied is the same as specified when in the
     * original content-length.
     *
     * @param out stream to copy to
     * @throws IOException throw when there is a problem writing to the streams
     */
    private void copyContentToOutputStream(final OutputStream out) throws IOException {
        final long bytesCopied;

        /* Only the EmbeddedHttpContent class requires us to actually call
         * write out on the wrapped object. In its particular case it is doing
         * a wrapping operation between an InputStream and an OutputStream in
         * order to provide an OutputStream interface to MantaClient. */
        if (this.wrapped.getClass().equals(EmbeddedHttpContent.class)) {
            CountingOutputStream cout = new CountingOutputStream(out);
            this.wrapped.writeTo(cout);
            cout.flush();
            bytesCopied = cout.getByteCount();
        } else {
            /* We choose a small buffer because large buffer don't result in
             * better performance when writing to a CipherOutputStream. You
             * can try this yourself by fiddling with this value and running
             * EncryptingEntityBenchmark. */
            final int bufferSize = 128;

            bytesCopied = IOUtils.copy(getContent(), out, bufferSize);
            out.flush();
        }

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

    /**
     * Initializes the cipher with an IV (initialization vector), so that
     * the cipher is ready to be used to encrypt.
     */
    private void initializeCipher() {
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
    }
}