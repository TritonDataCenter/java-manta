package com.joyent.manta.client.crypto;

import com.joyent.manta.client.multipart.MultipartOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

// FIXME: much dupe
public class EncryptingPartEntity implements HttpEntity {
    private static final Logger LOGGER = LoggerFactory.getLogger(EncryptingPartEntity.class);

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
     * Total length of the stream in bytes.
     */
    private long originalLength;


    private final OutputStream cipherStream;

    /**
     * Underlying entity that is being encrypted.
     */
    private final HttpEntity wrapped;

    private final MultipartOutputStream multipartStream;


    public EncryptingPartEntity(final OutputStream cipherStream,
                                final MultipartOutputStream multipartStream,
                                final HttpEntity wrapped) {
        this.cipherStream = cipherStream;
        this.multipartStream = multipartStream;

        //this.originalLength = wrapped.getContentLength();
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
        // if content length is not exact, then we will hang on a socket timeout
        return UNKNOWN_LENGTH;
        // if (originalLength >= 0) {
        //     return eContext.getCipherDetails().ciphertextSize(originalLength);
        // } else {
        //     return UNKNOWN_LENGTH;
        // }
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
        multipartStream.setNext(httpOut);
        try {
            //httpOut.write("foo".getBytes("UTF-8"));
            final int bufferSize = 128;
            long bytesCopied = IOUtils.copy(getContent(), cipherStream, bufferSize);
            cipherStream.flush();
            // how to close on final?
            /* We don't close quietly because we want the operation to fail if
             * there is an error closing out the CipherOutputStream. */
            //out.close();

            // if (out instanceof HmacOutputStream) {
            //     byte[] hmacBytes = out.getHmac().doFinal();
            //     Validate.isTrue(hmacBytes.length == eContext.getCipherDetails().getAuthenticationTagOrHmacLengthInBytes(),
            //             "HMAC actual bytes doesn't equal the number of bytes expected");

            //     if (LOGGER.isDebugEnabled()) {
            //         LOGGER.debug("HMAC: {}", Hex.encodeHexString(hmacBytes));
            //     }

            //     httpOut.write(hmacBytes);
            // }
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
