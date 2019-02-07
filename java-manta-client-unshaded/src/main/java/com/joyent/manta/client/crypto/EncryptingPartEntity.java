/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import com.joyent.manta.client.multipart.MultipartOutputStream;
import com.joyent.manta.exception.MantaIOException;
import com.joyent.manta.exception.MantaResourceCloseException;
import com.joyent.manta.http.MantaContentTypes;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.lang3.Validate;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
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
     * Callback function for for the "last" part.
     */
    private final LastPartCallback lastPartCallback;

    /**
     * Creates a new instance based on the specified parameters.
     *
     * @param cipherStream encrypting stream
     * @param multipartStream multipart stream that allows to attaching / detaching streams
     * @param wrapped wrapped entity to encrypt and send
     * @param lastPartCallback callback for the last part (nullable)
     */
    public EncryptingPartEntity(final OutputStream cipherStream,
                                final MultipartOutputStream multipartStream,
                                final HttpEntity wrapped,
                                final LastPartCallback lastPartCallback) {
        this.cipherStream = cipherStream;
        this.multipartStream = multipartStream;
        this.wrapped = wrapped;
        this.lastPartCallback = lastPartCallback;
    }

    @Override
    public boolean isRepeatable() {
        return false;
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
        Validate.notNull(httpOut, "HttpOut stream must not be null");
        Validate.notNull(multipartStream, "MultipartStream must not be null");

        multipartStream.setNext(httpOut);

        final InputStream contentStream = getContent();
        Validate.notNull(contentStream, "Content input stream must not be null");
        Validate.notNull(cipherStream, "Cipher output stream must not be null");
        final CountingOutputStream cout = new CountingOutputStream(cipherStream);

        try {
            IOUtils.copy(contentStream, cout, BUFFER_SIZE);

            if (lastPartCallback == null) {
                cout.flush();
            } else {
                final long bytesWritten = cout.getByteCount();
                // This call will close cout/cipherStream
                final ByteArrayOutputStream remainderStream = lastPartCallback.call(bytesWritten);

                if (remainderStream.size() > 0) {
                    final byte[] remainder = remainderStream.toByteArray();
                    httpOut.write(remainder);
                }
            }
        } catch (Exception e) {
            // Attempt to close the input stream and in the case where we can't
            // we log because we can't deal with two exceptions at once.
            try {
                contentStream.close();
            } catch (IOException ioe) {
                LoggerFactory.getLogger(getClass()).error(
                        "Unable to close the encrypting part entity content stream", e);
            }

            if (e instanceof IOException) {
                String msg = "Unable to copy multipart source data to Manta";
                MantaIOException mio = new MantaIOException(msg, e);
                mio.setContextValue("contentStream", contentStream);
                mio.setContextValue("bytesWritten", cout.getByteCount());
                throw mio;
            } else {
                throw e;
            }
        }

        /* If an exception is thrown on close() here, it will propagate up
         * the stack through the line where httpClient.execute() statement
         * was invoked. We leave the handling of any exceptions related to
         * closing this stream up to the user and intentionally do not
         * suppress them. The user may catch it specifically by catching the
         * MantaResourceCloseException type.
         */
        try {
            contentStream.close();
        } catch (IOException e) {
            String msg = "Unable to close the encrypting part entity content stream";
            MantaResourceCloseException mrce = new MantaResourceCloseException(msg, e);
            mrce.setContextValue("contentStream", contentStream);
            mrce.setContextValue("bytesWritten", cout.getByteCount());

            throw mrce;
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

    /**
     * "Callback" function object.
     */
    public abstract static class LastPartCallback {

        /**
         * When a user has uploaded the last part some action will
         * need to be taken, such as flushing a the buffer or writing
         * an HMAC.  If the user uploads a part that is less than the
         * minimum size then we can detect that it is the last part at
         * that time and append the relevant bytes.  This callback
         * allows a MultipartManager to check if the upload byte
         * count, and then return whatever additional bytes need to be
         * uploaded.
         *
         * @param uploadedBytes How many bytes have been uploaded for this part
         * @return The remaining bytes to upload, or a zero length
         * stream if there are none.
         * @throws IOException If here was an error constructing the remainder stream.
         */
        public abstract ByteArrayOutputStream call(long uploadedBytes) throws IOException;
    }
}
