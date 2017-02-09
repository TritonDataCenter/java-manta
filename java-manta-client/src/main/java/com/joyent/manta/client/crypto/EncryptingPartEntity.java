/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import com.joyent.manta.client.multipart.MultipartOutputStream;
import com.joyent.manta.http.MantaContentTypes;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Class that provides an interface for writing multipart parts to a
 * HTTP socket represented as a {@link OutputStream}.
 */
public class EncryptingPartEntity implements HttpEntity {
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
     * Buffer size when copying from input to output (cipher) stream.
     */
    private static final int BUFFER_SIZE = 128;

    /**
     * Encrypting stream.
     */
    private final OutputStream cipherStream;

    /**
     * Underlying entity that is being encrypted.
     */
    private final HttpEntity wrapped;

    /**
     * Multipart stream that allows to attaching / detaching streams.
     */
    private final MultipartOutputStream multipartStream;

    /**
     * Creates a new instance based on the specified parameters.
     *
     * @param cipherStream encrypting stream
     * @param multipartStream multipart stream that allows to attaching / detaching streams
     * @param wrapped wrapped entity to encrypt and send
     */
    public EncryptingPartEntity(final OutputStream cipherStream,
                                final MultipartOutputStream multipartStream,
                                final HttpEntity wrapped) {
        this.cipherStream = cipherStream;
        this.multipartStream = multipartStream;
        this.wrapped = wrapped;
    }

    @Override
    public boolean isRepeatable() {
        return wrapped.isRepeatable();
    }

    @Override
    public boolean isChunked() {
        return true;
    }

    @Override
    public long getContentLength() {
        // if content length is not exact, then we will hang on a socket timeout
        return UNKNOWN_LENGTH;
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
        multipartStream.setNext(httpOut);
        try {
            IOUtils.copy(getContent(), cipherStream, BUFFER_SIZE);
            cipherStream.flush();
        } finally {
            IOUtils.closeQuietly(httpOut);
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
}
