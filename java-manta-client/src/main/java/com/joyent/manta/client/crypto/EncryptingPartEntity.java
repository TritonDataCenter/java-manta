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

// FIXME: much dupe
public class EncryptingEntity implements HttpEntity {
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


    private final EncryptionContext eContext;

    /**
     * Underlying entity that is being encrypted.
     */
    private final HttpEntity wrapped;


    public EncryptingPartEntity(final EncryptionContext eContext, MultipartOutputStream multiPartSteram,
                                final HttpEntity wrapped) {
        // if (originalLength > cipherDetails.getMaximumPlaintextSizeInBytes()) {
        //     String msg = String.format("Input content length exceeded maximum "
        //     + "[%d] number of bytes supported by cipher [%s]",
        //             cipherDetails.getMaximumPlaintextSizeInBytes(),
        //             cipherDetails.getCipherAlgorithm());
        //     throw new MantaClientEncryptionException(msg);
        // }

        this.multiPartStream = multiPartStream;
        this.eContext = eContext;

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
        if (originalLength >= 0) {
            return eContext.getCipherDetails().ciphertextSize(originalLength);
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
        out = EncryptingEntityHelper.makeCipherOutputforStream(httpOut, eContext);
        try {
            final int bufferSize = 128;
            int bytesCopied = IOUtils.copy(getContent(), multiPartStream, bufferSize);
            out.flush();
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
        return eContext.getCipher();
    }

}
