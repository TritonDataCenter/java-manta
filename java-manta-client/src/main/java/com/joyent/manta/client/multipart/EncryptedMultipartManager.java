/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.crypto.AesCtrCipherDetails;
import com.joyent.manta.client.crypto.EncryptingEntityHelper;
import com.joyent.manta.client.crypto.EncryptingPartEntity;
import com.joyent.manta.client.crypto.EncryptionContext;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.exception.MantaMultipartException;
import com.joyent.manta.http.EncryptionHttpHelper;
import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.commons.lang3.Validate;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
public class EncryptedMultipartManager
        <WRAPPED_MANAGER extends AbstractMultipartManager<WRAPPED_UPLOAD, ? extends MantaMultipartUploadPart>,
         WRAPPED_UPLOAD extends MantaMultipartUpload>
        extends AbstractMultipartManager<EncryptedMultipartUpload<WRAPPED_UPLOAD>,
                                         MantaMultipartUploadPart> {

    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(EncryptedMultipartManager.class);

    /**
     * Secret key used for encryption.
     */
    private final SecretKey secretKey;

    /**
     * Cipher/mode properties object.
     */
    private final SupportedCipherDetails cipherDetails;

    /**
     * Encryption operations for HTTP connections helper class.
     */
    private final EncryptionHttpHelper httpHelper;

    /**
     * Backing multipart instance.
     */
    private final WRAPPED_MANAGER wrapped;

    /**
     * Creates a new encrypting multipart manager class backed by the specified
     * Manta client and wrapping the underlying manager.
     *
     * @param mantaClient manta client instance
     * @param wrapped instance of underlying wrapper
     */
    public EncryptedMultipartManager(final MantaClient mantaClient,
                                     final WRAPPED_MANAGER wrapped) {
        Validate.notNull(mantaClient, "Manta client must not be null");
        Validate.notNull(wrapped, "Wrapped manager must not be null");

        this.httpHelper = readFieldFromMantaClient("httpHelper",
                mantaClient, EncryptionHttpHelper.class);
        this.wrapped = wrapped;
        this.secretKey = this.httpHelper.getSecretKey();
        this.cipherDetails = this.httpHelper.getCipherDetails();

        if (!(this.cipherDetails instanceof AesCtrCipherDetails)) {
            throw new UnsupportedOperationException("Currently only AES/CTR "
                    + "cipher/modes are supported for encrypted multipart uploads");
        }
    }

    /**
     * Creates a new encrypting multipart manager class back by the specified
     * classes.
     *
     * @param secretKey secret key used to encrypt
     * @param cipherDetails cipher/mode properties object
     * @param httpHelper encryption operations for HTTP connections helper class
     * @param wrapped backing multipart class
     */
    EncryptedMultipartManager(final SecretKey secretKey,
                              final SupportedCipherDetails cipherDetails,
                              final EncryptionHttpHelper httpHelper,
                              final WRAPPED_MANAGER wrapped) {
        this.secretKey = secretKey;
        this.cipherDetails = cipherDetails;
        this.wrapped = wrapped;
        this.httpHelper = httpHelper;

        if (!(this.cipherDetails instanceof AesCtrCipherDetails)) {
            throw new UnsupportedOperationException("Currently only AES/CTR "
                    + "cipher/modes are supported for encrypted multipart uploads");
        }

        /* When combining CSE and MPU, an additional buffering layer
         * is added via MultipartOutputStream.  If the minimum part
         * size did not align with the cipher block size, a user could
         * upload what they thought was a large enough part, but be
         * presented with an error when the client actually uploaded a
         * few bytes left due to the buffer.
         */
        assert getMinimumPartSize() == 1 ||  getMinimumPartSize() % cipherDetails.getBlockSizeInBytes() == 0;
    }

    @Override
    public int getMaxParts() {
        // We need one part for the HMAC, so the maximum will always be one less
        return this.wrapped.getMaxParts() - 1;
    }

    @Override
    public int getMinimumPartSize() {
        return this.wrapped.getMinimumPartSize();
    }

    @Override
    public EncryptedMultipartUpload<WRAPPED_UPLOAD> initiateUpload(final String path)
            throws IOException {
        return initiateUpload(path, new MantaMetadata());
    }

    @Override
    public EncryptedMultipartUpload<WRAPPED_UPLOAD> initiateUpload(final String path,
                                                                   final MantaMetadata mantaMetadata)
            throws IOException {
        return initiateUpload(path, mantaMetadata, new MantaHttpHeaders());
    }

    @Override
    public EncryptedMultipartUpload<WRAPPED_UPLOAD> initiateUpload(final String path,
                                                                   final MantaMetadata mantaMetadata,
                                                                   final MantaHttpHeaders httpHeaders)
            throws IOException {
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

        final MantaMetadata metadata;

        if (mantaMetadata == null) {
            metadata = new MantaMetadata();
        } else {
            metadata = mantaMetadata;
        }

        final MantaHttpHeaders headers;

        if (httpHeaders == null) {
            headers = new MantaHttpHeaders();
        } else {
            headers = httpHeaders;
        }

        final EncryptionContext encryptionContext = buildEncryptionContext();
        final Cipher cipher = encryptionContext.getCipher();

        httpHelper.attachEncryptionCipherHeaders(metadata);
        httpHelper.attachEncryptedMetadata(metadata);
        httpHelper.attachEncryptedEntityHeaders(metadata, cipher);
        /* We remove the e- prefixed metadata because we have already
         * encrypted it and stored it in m-encrypt-metadata. */
        metadata.removeAllEncrypted();

        /* If content-length is specified, we store the content length as
         * the plaintext content length on the object's metadata and then
         * remove the header because server-side MPU will attempt to verify
         * that the content-length in the header is the same as the ciphertext's
         * length. This won't work because the ciphertext length will always
         * be different than the plaintext content length. */
        if (headers.getContentLength() != null && headers.getContentLength() > -1) {
            metadata.put(MantaHttpHeaders.ENCRYPTION_PLAINTEXT_CONTENT_LENGTH,
                    headers.getContentLength().toString());
            headers.remove(HttpHeaders.CONTENT_LENGTH);
        } else if (contentLength != null && contentLength > -1) {
            metadata.put(MantaHttpHeaders.ENCRYPTION_PLAINTEXT_CONTENT_LENGTH,
                    contentLength.toString());
        }

        /* If the Content-MD5 header is set, it will always be inaccurate because
         * the ciphertext will have a different checksum and it will cause the
         * server-side checksum verification to fail. */
        if (headers.getContentMD5() != null) {
            headers.remove(HttpHeaders.CONTENT_MD5);
        }

        WRAPPED_UPLOAD upload = wrapped.initiateUpload(path, metadata, headers);

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
        final EncryptionContext encryptionContext = encryptionState.getEncryptionContext();

        encryptionState.getLock().lock();
        try {
            validatePartNumber(partNumber);
            if (encryptionState.getLastPartNumber() != -1) {
                Validate.isTrue(encryptionState.getLastPartNumber() + 1 == partNumber,
                        "Encrypted MPU parts must be serial and sequential");
            } else {
                encryptionState.setMultipartStream(new MultipartOutputStream(
                        encryptionContext.getCipherDetails().getBlockSizeInBytes()));
                encryptionState.setCipherStream(EncryptingEntityHelper.makeCipherOutputForStream(
                        encryptionState.getMultipartStream(), encryptionContext));
            }

            final EncryptingPartEntity entity = new EncryptingPartEntity(
                    encryptionState.getCipherStream(),
                    encryptionState.getMultipartStream(), sourceEntity,
                    new EncryptingPartEntity.LastPartCallback() {
                        @Override
                        public ByteArrayOutputStream call(final long uploadedBytes) throws IOException {
                            if (uploadedBytes < wrapped.getMinimumPartSize()) {
                                LOGGER.debug("Detected part {} as last part based on size", partNumber);
                                return encryptionState.remainderAndLastPartAuth();
                            } else {
                                return new ByteArrayOutputStream();
                            }
                        }
                    });
            encryptionState.setLastPartNumber(partNumber);
            return wrapped.uploadPart(upload.getWrapped(), partNumber, entity);
        } finally {
            encryptionState.getLock().unlock();
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
        final EncryptionContext encryptionContext = encryptionState.getEncryptionContext();

        encryptionState.getLock().lock();
        try {
            Stream<? extends MantaMultipartUploadTuple> finalPartsStream = partsStream;
            if (!encryptionState.isLastPartAuthWritten()) {
                ByteArrayOutputStream remainderStream = encryptionState.remainderAndLastPartAuth();
                if (remainderStream.size() > 0) {
                    MantaMultipartUploadPart finalPart = wrapped.uploadPart(upload.getWrapped(),
                                                                            encryptionState.getLastPartNumber() + 1,
                                                                            remainderStream.toByteArray());
                    finalPartsStream = Stream.concat(partsStream, Stream.of(finalPart));
                }
            }

            wrapped.complete(upload.getWrapped(), finalPartsStream);
        } finally {
            encryptionState.getLock().unlock();
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
    public void abort(final EncryptedMultipartUpload<WRAPPED_UPLOAD> upload) throws IOException {
        wrapped.abort(upload.getWrapped());
    }

    /**
     * Builds a new instance of an encryption context based on the state
     * of the current {@link EncryptedMultipartManager} instance.
     *
     * @return configured encryption context object
     */
    private EncryptionContext buildEncryptionContext() {
        return new EncryptionContext(this.secretKey, this.cipherDetails);
    }

    /**
     * @return a reference to the underlying backing MPU implementation
     */
    public WRAPPED_MANAGER getWrapped() {
        return wrapped;
    }
}
