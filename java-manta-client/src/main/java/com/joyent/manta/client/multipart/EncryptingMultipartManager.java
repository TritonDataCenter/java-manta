/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client.multipart;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.crypto.EncryptingEntityHelper;
import com.joyent.manta.client.crypto.EncryptingPartEntity;
import com.joyent.manta.client.crypto.EncryptionContext;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.exception.MantaMultipartException;
import com.joyent.manta.http.EncryptionHttpHelper;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.util.HmacOutputStream;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Multipart upload manager class that wraps another {@link MantaMultipartManager}
 * instance and transparently encrypts the contents of all files uploaded.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 *
 * @param <WRAPPED_MANAGER> Manager class to wrap
 * @param <WRAPPED_UPLOAD> Upload class to wrap
 */
public class EncryptingMultipartManager
        <WRAPPED_MANAGER extends AbstractMultipartManager<WRAPPED_UPLOAD, ? extends MantaMultipartUploadPart>,
         WRAPPED_UPLOAD extends MantaMultipartUpload>
        extends AbstractMultipartManager<EncryptedMultipartUpload<WRAPPED_UPLOAD>,
                                         MantaMultipartUploadPart> {

    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(EncryptingMultipartManager.class);

    private final SecretKey secretKey;
    private final SupportedCipherDetails cipherDetails;
    private final EncryptionHttpHelper httpHelper;
    private final WRAPPED_MANAGER wrapped;

    public EncryptingMultipartManager(final MantaClient mantaClient,
                                      final WRAPPED_MANAGER wrapped) {
        Validate.notNull(mantaClient, "Manta client must not be null");
        Validate.notNull(wrapped, "Wrapped manager must not be null");

        final Field httpHelperField = FieldUtils.getField(MantaClient.class,
                "httpHelper", true);
        try {
            Object httpHelperObject = FieldUtils.readField(httpHelperField, mantaClient);
            this.httpHelper = (EncryptionHttpHelper)httpHelperObject;
        } catch (IllegalAccessException e) {
            throw new MantaMultipartException("Unabled to access httpHelper "
                    + "field on MantaClient");
        }

        this.wrapped = wrapped;
        this.secretKey = this.httpHelper.secretKey;
        this.cipherDetails = this.httpHelper.getCipherDetails();
    }

    public EncryptingMultipartManager(final SecretKey secretKey,
                                      final SupportedCipherDetails cipherDetails,
                                      final EncryptionHttpHelper httpHelper,
                                      final WRAPPED_MANAGER wrapped) {
        this.secretKey = secretKey;
        this.cipherDetails = cipherDetails;
        this.wrapped = wrapped;
        this.httpHelper = httpHelper;
    }

    @Override
    public int getMaxParts() {
        // We need one part for the HMAC, so the maximum will always be one less
        return this.wrapped.getMaxParts() - 1;
    }

    @Override
    public EncryptedMultipartUpload<WRAPPED_UPLOAD> initiateUpload(String path) throws IOException {
        return initiateUpload(path, new MantaMetadata());
    }

    @Override
    public EncryptedMultipartUpload<WRAPPED_UPLOAD> initiateUpload(String path, MantaMetadata mantaMetadata) throws IOException {
        return initiateUpload(path, mantaMetadata, new MantaHttpHeaders());
    }

    @Override
    public EncryptedMultipartUpload<WRAPPED_UPLOAD> initiateUpload(final String path,
                                                                   final MantaMetadata mantaMetadata,
                                                                   final MantaHttpHeaders httpHeaders) throws IOException {
        return initiateUpload(path, null, mantaMetadata, httpHeaders);
    }

    @Override
    public EncryptedMultipartUpload<WRAPPED_UPLOAD> initiateUpload(final String path,
                                                                   final Long contentLength,
                                                                   final MantaMetadata mantaMetadata,
                                                                   final MantaHttpHeaders httpHeaders)
            throws IOException {
        Validate.notNull(path, "Path to object must not be null");
        Validate.notBlank(path, "Path to object must not be blank");
        Validate.notNull(mantaMetadata, "Metadata object must not be null");
        Validate.notNull(httpHeaders, "HTTP headers object must not be null");

        final EncryptionContext encryptionContext = buildEncryptionContext();
        final Cipher cipher = encryptionContext.getCipher();

        httpHelper.attachEncryptionCipherHeaders(mantaMetadata);
        httpHelper.attachEncryptedMetadata(mantaMetadata);
        httpHelper.attachEncryptedEntityHeaders(mantaMetadata, cipher);
        /* We remove the e- prefixed metadata because we have already
         * encrypted it and stored it in m-encrypt-metadata. */
        mantaMetadata.removeAllEncrypted();

        /* If content-length is specified, we store the content length as
         * the plaintext content length on the object's metadata and then
         * remove the header because server-side MPU will attempt to verify
         * that the content-length in the header is the same as the ciphertext's
         * length. This won't work because the ciphertext length will always
         * be different than the plaintext content length. */
        if (httpHeaders.getContentLength() != null && httpHeaders.getContentLength() > -1) {
            mantaMetadata.put(MantaHttpHeaders.ENCRYPTION_PLAINTEXT_CONTENT_LENGTH,
                    httpHeaders.getContentLength().toString());
            httpHeaders.remove(HttpHeaders.CONTENT_LENGTH);
        } else if (contentLength != null && contentLength > -1) {
            mantaMetadata.put(MantaHttpHeaders.ENCRYPTION_PLAINTEXT_CONTENT_LENGTH,
                    contentLength.toString());
        }

        /* If the Content-MD5 header is set, it will always be inaccurate because
         * the ciphertext will have a different checksum and it will cause the
         * server-side checksum verification to fail. */
        if (httpHeaders.getContentMD5() != null) {
            httpHeaders.remove(HttpHeaders.CONTENT_MD5);
        }

        WRAPPED_UPLOAD upload = wrapped.initiateUpload(path, mantaMetadata, httpHeaders);

        EncryptionState encryptionState = new EncryptionState(encryptionContext);
        EncryptedMultipartUpload<WRAPPED_UPLOAD> encryptedUpload =
                new EncryptedMultipartUpload<>(upload, encryptionState);

        LOGGER.debug("Created new encrypted multipart upload: {}", upload);

        return encryptedUpload;
    }

    @Override
    public Stream<MantaMultipartUpload> listInProgress() throws IOException {
        return wrapped.listInProgress();
    }

    @Override
    protected MantaMultipartUploadPart uploadPart(final EncryptedMultipartUpload<WRAPPED_UPLOAD> upload,
                                                  final int partNumber,
                                                  final HttpEntity sourceEntity) throws IOException {
        Validate.notNull(upload, "Multipart upload object must not be null");

        if (!upload.canUpload()) {
            String msg = "Multipart object is not in a state that it can be "
                    + "used for uploading parts. For encrypted multipart "
                    + "uploads, only multipart upload objects returned from "
                    + "the initiateUpload() method can be used to upload parts.";
            throw new IllegalArgumentException(msg);
        }

        final EncryptionState encryptionState = upload.getEncryptionState();
        encryptionState.lock.lock();

        final EncryptionContext encryptionContext = encryptionState.eContext;
        final SupportedCipherDetails cipherDetails = encryptionContext.getCipherDetails();

        try {
            validatePartNumber(partNumber);
            if (encryptionState.lastPartNumber != -1) {
                Validate.isTrue(encryptionState.lastPartNumber + 1 == partNumber,
                        "Encrypted MPU parts must be serial and sequential");
            } else {
                encryptionState.multipartStream = new MultipartOutputStream(cipherDetails.getBlockSizeInBytes());
                encryptionState.cipherStream = EncryptingEntityHelper.makeCipherOutputforStream(
                        encryptionState.multipartStream, encryptionContext);
            }

            final EncryptingPartEntity entity = new EncryptingPartEntity(
                    encryptionState.cipherStream,
                    encryptionState.multipartStream, sourceEntity);
            encryptionState.lastPartNumber = partNumber;
            return wrapped.uploadPart(upload.getWrapped(), partNumber, entity);
        } finally {
            encryptionState.lock.unlock();
        }
    }

    @Override
    public void complete(final EncryptedMultipartUpload<WRAPPED_UPLOAD> upload,
                         final Iterable<? extends MantaMultipartUploadTuple> parts)
            throws IOException {
        try (Stream<? extends MantaMultipartUploadTuple> stream =
                     StreamSupport.stream(parts.spliterator(), false)) {
            complete(upload, stream);
        }
    }

    @Override
    public void complete(final EncryptedMultipartUpload<WRAPPED_UPLOAD> upload,
                         final Stream<? extends MantaMultipartUploadTuple> partsStream)
            throws IOException {
        final EncryptionState encryptionState = upload.getEncryptionState();

        encryptionState.lock.lock();

        final EncryptionContext encryptionContext = encryptionState.eContext;
        final SupportedCipherDetails cipherDetails = encryptionContext.getCipherDetails();

        try {
            Stream<? extends MantaMultipartUploadTuple> finalPartsStream = partsStream;
            ByteArrayOutputStream remainderStream = new ByteArrayOutputStream();
            encryptionState.multipartStream.setNext(remainderStream);
            encryptionState.cipherStream.close();
            remainderStream.write(encryptionState.multipartStream.getRemainder());

            // conditionally get hmac and upload part; yeah reenterant lock
            if (encryptionState.cipherStream.getClass().equals(HmacOutputStream.class)) {
                byte[] hmacBytes = ((HmacOutputStream) encryptionState.cipherStream).getHmac().doFinal();
                Validate.isTrue(hmacBytes.length == cipherDetails.getAuthenticationTagOrHmacLengthInBytes(),
                        "HMAC actual bytes doesn't equal the number of bytes expected");

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("HMAC: {}", Hex.encodeHexString(hmacBytes));
                }
                remainderStream.write(hmacBytes);
            }

            if (remainderStream.size() > 0) {
                MantaMultipartUploadPart finalPart = wrapped.uploadPart(upload.getWrapped(),
                        encryptionState.lastPartNumber + 1, remainderStream.toByteArray());
                finalPartsStream = Stream.concat(partsStream, Stream.of(finalPart));
            }

            wrapped.complete(upload.getWrapped(), finalPartsStream);
        } finally {
            encryptionState.lock.unlock();
        }
    }

    @Override
    public MantaMultipartUploadPart getPart(final EncryptedMultipartUpload<WRAPPED_UPLOAD> upload,
                                            final int partNumber) throws IOException {
        return wrapped.getPart(upload.getWrapped(), partNumber);
    }

    @Override
    public MantaMultipartStatus getStatus(final EncryptedMultipartUpload<WRAPPED_UPLOAD> upload)
            throws IOException {
        return wrapped.getStatus(upload.getWrapped());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Stream<MantaMultipartUploadPart> listParts(final EncryptedMultipartUpload<WRAPPED_UPLOAD> upload)
            throws IOException {
        return (Stream<MantaMultipartUploadPart>)wrapped.listParts(upload.getWrapped());
    }

    @Override
    public void validateThatThereAreSequentialPartNumbers(final EncryptedMultipartUpload<WRAPPED_UPLOAD> upload)
            throws IOException, MantaMultipartException {
        wrapped.validateThatThereAreSequentialPartNumbers(upload.getWrapped());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void abort(final EncryptedMultipartUpload<WRAPPED_UPLOAD> upload) throws IOException {
        wrapped.abort(upload.getWrapped());
    }

    private EncryptionContext buildEncryptionContext() {
        return new EncryptionContext(secretKey, cipherDetails);
    }

    public WRAPPED_MANAGER getWrapped() {
        return wrapped;
    }
}
