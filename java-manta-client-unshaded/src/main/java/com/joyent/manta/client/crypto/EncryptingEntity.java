/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import com.joyent.manta.exception.MantaClientEncryptionException;
import com.joyent.manta.exception.MantaIOException;
import com.joyent.manta.http.MantaContentTypes;
import com.joyent.manta.http.entity.EmbeddedHttpContent;
import com.joyent.manta.util.HmacOutputStream;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.lang3.Validate;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.bouncycastle.crypto.macs.HMac;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;

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
            HttpHeaders.CONTENT_TYPE, MantaContentTypes.ENCRYPTED_OBJECT.toString());

    /**
     * Total length of the stream in bytes.
     */
    private long originalLength;

    /**
     * Running state of the cipher used for encrypting the plaintext input.
     */
    private EncryptionContext encryptionContext;

    /**
     * Underlying entity that is being encrypted.
     */
    private final HttpEntity wrapped;


    /**
     * Creates a new instance with an known stream size. We initialize an {@link EncryptionContext} here
     * (even though it will be replaced at the beginning of {@link EncryptingEntity#writeTo(OutputStream)})
     * in order to generate an IV for the header through {@link EncryptingEntity#getCipher()} and to allow
     * usage of {@link EncryptingEntity#getContentLength()}
     *
     * @param key key to encrypt stream with
     * @param cipherDetails cipher to encrypt stream with
     * @param wrapped underlying stream to encrypt
     */
    public EncryptingEntity(final SecretKey key,
                            final SupportedCipherDetails cipherDetails,
                            final HttpEntity wrapped) {
        if (originalLength > cipherDetails.getMaximumPlaintextSizeInBytes()) {
            String msg = String.format("Input content length exceeded maximum "
            + "[%d] number of bytes supported by cipher [%s]",
                    cipherDetails.getMaximumPlaintextSizeInBytes(),
                    cipherDetails.getCipherAlgorithm());
            throw new MantaClientEncryptionException(msg);
        }

        this.encryptionContext = new EncryptionContext(key, cipherDetails);

        this.originalLength = wrapped.getContentLength();
        this.wrapped = wrapped;
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
            return encryptionContext.getCipherDetails().ciphertextSize(originalLength);
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
         /*
          * Construct a fresh EncryptionContext each time we attempt to write out the entity.
          * Calling encryptionContext.initializeCipher(getCipher().getIV()) from here would be ideal
          * but is not compatible with AES-GCM
          */
        this.encryptionContext = new EncryptionContext(
                encryptionContext.getSecretKey(),
                encryptionContext.getCipherDetails(),
                encryptionContext.getCipher().getIV());

        OutputStream out = EncryptingEntityHelper.makeCipherOutputForStream(
                httpOut, encryptionContext);
        try {
            copyContentToOutputStream(out);
            /* We don't close quietly because we want the operation to fail if
             * there is an error closing out the CipherOutputStream. */
            out.close();

            if (out instanceof HmacOutputStream) {
                final HMac hmac = ((HmacOutputStream) out).getHmac();
                final int hmacSize = hmac.getMacSize();
                final byte[] hmacBytes = new byte[hmacSize];
                hmac.doFinal(hmacBytes, 0);

                Validate.isTrue(hmacBytes.length == hmacSize,
                        "HMAC actual bytes doesn't equal the number of bytes expected");

                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("HMAC: {}", Hex.encodeHexString(hmacBytes));
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

            InputStream contentStream = getContent();
            bytesCopied = IOUtils.copy(contentStream, out, bufferSize);
            out.flush();
            try {
                contentStream.close();
            } catch (IOException e) {
                LOGGER.error("Failed to close content stream in EncryptingEntity.", e);
            }
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
        return encryptionContext.getCipher();
    }

}
