/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client.multipart;

import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.crypto.EncryptionContext;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.exception.MantaClientEncryptionException;
import com.joyent.manta.exception.MantaMultipartException;
import com.joyent.manta.http.EncryptionHttpHelper;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.util.HmacInputStream;
import org.apache.commons.codec.Charsets;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.Validate;
import org.apache.http.HttpHeaders;
import org.bouncycastle.jcajce.io.CipherInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.spec.AlgorithmParameterSpec;
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
        <WRAPPED_MANAGER extends MantaMultipartManager<WRAPPED_UPLOAD, ? extends MantaMultipartUploadPart>,
         WRAPPED_UPLOAD extends MantaMultipartUpload>
        implements MantaMultipartManager<EncryptedMultipartUpload<WRAPPED_UPLOAD>,
                                         MantaMultipartUploadPart> {

    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(EncryptingMultipartManager.class);

    // need one part for hmac
    public static final int MAX_PARTS = MantaMultipartManager.MAX_PARTS - 1;

    private final EncryptionContext encryptionContext;
    private final EncryptionHttpHelper httpHelper;
    private final WRAPPED_MANAGER wrapped;

    public EncryptingMultipartManager(final EncryptionContext encryptionContext,
                                      final EncryptionHttpHelper httpHelper,
                                      final WRAPPED_MANAGER wrapped) {
        this.encryptionContext = encryptionContext;
        this.wrapped = wrapped;
        this.httpHelper = httpHelper;
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
                                                                   final MantaHttpHeaders httpHeaders) throws IOException {
        Validate.notNull(path, "Path to object must not be null");
        Validate.notBlank(path, "Path to object must not be blank");
        Validate.notNull(mantaMetadata, "Metadata object must not be null");
        Validate.notNull(httpHeaders, "HTTP headers object must not be null");

        final SupportedCipherDetails cipherDetails = encryptionContext.getCipherDetails();
        final Cipher cipher = cipherDetails.getCipher();
        final byte[] iv = cipherDetails.generateIv();

        try {
            AlgorithmParameterSpec spec = cipherDetails.getEncryptionParameterSpec(iv);
            cipher.init(Cipher.ENCRYPT_MODE, encryptionContext.getSecretKey(), spec);
        } catch (GeneralSecurityException e) {
            String msg = "Unable to initialize cipher";
            MantaClientEncryptionException mcee = new MantaClientEncryptionException(msg, e);
            mcee.setContextValue("cipherDetails", cipherDetails);
            mcee.setContextValue("objectPath", path);
            mcee.setContextValue("iv", Hex.encodeHexString(iv));

            throw mcee;
        }

        final Mac hmac;

        if (cipherDetails.isAEADCipher()) {
            hmac = null;
        } else {
            hmac = cipherDetails.getAuthenticationHmac();

            try {
                hmac.init(encryptionContext.getSecretKey());
            } catch (InvalidKeyException e) {
                String msg = "Couldn't initialize HMAC with secret key";
                throw new MantaClientEncryptionException(msg, e);
            }

            hmac.update(iv);
        }

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
        if (httpHeaders.getContentLength() != null) {
            mantaMetadata.put(MantaHttpHeaders.ENCRYPTION_PLAINTEXT_CONTENT_LENGTH,
                    httpHeaders.getContentLength().toString());
            httpHeaders.remove(HttpHeaders.CONTENT_LENGTH);
        } else if (contentLength != null) {
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

        EncryptedMultipartUpload<WRAPPED_UPLOAD> encryptedUpload =
                new EncryptedMultipartUpload<>(upload, cipher, hmac);

        return encryptedUpload;
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
        final SupportedCipherDetails cipherDetails = encryptionContext.getCipherDetails();

        final Stream<? extends MantaMultipartUploadTuple> finalPartsStream;

        if (cipherDetails.isAEADCipher()) {
            finalPartsStream = partsStream;
        } else {
            final byte[] hmac = upload.getHmac().doFinal();
            MantaMultipartUploadPart hmacPart = uploadPart(
                    upload, upload.getCurrentPartNumber() + 1, hmac);
            finalPartsStream = Stream.concat(partsStream, Stream.of(hmacPart));
        }

        wrapped.complete(upload.getWrapped(), finalPartsStream);
    }

    @Override
    public Stream<MantaMultipartUpload> listInProgress() throws IOException {
        return wrapped.listInProgress();
    }

    @Override
    public MantaMultipartUploadPart uploadPart(final EncryptedMultipartUpload<WRAPPED_UPLOAD> upload,
                                               final int partNumber,
                                               final String contents) throws IOException {
        InputStream in = new ByteArrayInputStream(contents.getBytes(Charsets.UTF_8));
        return uploadPart(upload, partNumber, in);
    }

    @Override
    public MantaMultipartUploadPart uploadPart(final EncryptedMultipartUpload<WRAPPED_UPLOAD> upload,
                                               final int partNumber,
                                               final byte[] bytes) throws IOException {
        InputStream in = new ByteArrayInputStream(bytes);
        return uploadPart(upload, partNumber, in);
    }

    @Override
    public MantaMultipartUploadPart uploadPart(final EncryptedMultipartUpload<WRAPPED_UPLOAD> upload,
                                               final int partNumber,
                                               final File file) throws IOException {
        InputStream in = new FileInputStream(file);
        return uploadPart(upload, partNumber, in);
    }

    @Override
    public synchronized MantaMultipartUploadPart uploadPart(final EncryptedMultipartUpload<WRAPPED_UPLOAD> upload,
                                                            final int partNumber,
                                                            final InputStream inputStream)
            throws IOException {
        Validate.notNull(upload, "Multipart upload object must not be null");

        if (!upload.canUpload()) {
            String msg = "The upload object specified can't be used for uploading parts "
                    + "because it was not created using the initiateUpload() method.";
            throw new IllegalArgumentException(msg);
        }

        if (partNumber != upload.getCurrentPartNumber() + 1) {
            String msg = String.format("The part number specified [%d] is not sequential"
                    + " from the last part's number [%d] uploaded",
                    partNumber, upload.getCurrentPartNumber());
            throw new IllegalArgumentException(msg);
        }

        final InputStream in;

        // TODO: Read in remainder

        if (upload.getLastBlockOverrunBytes() != null && upload.getLastBlockOverrunBytes().length > 0) {

        }

        // TODO: Add BoundedInputStream and store remainder

        final SupportedCipherDetails cipherDetails = encryptionContext.getCipherDetails();

        if (cipherDetails.isAEADCipher()) {
            in = new CipherInputStream(inputStream, upload.getCipher());
        } else {
            CipherInputStream cipherStream = new CipherInputStream(inputStream, upload.getCipher());
            in = new HmacInputStream(upload.getHmac(), cipherStream);
        }

        MantaMultipartUploadPart part = wrapped.uploadPart(upload.getWrapped(), partNumber, in);

        upload.incrementPartNumber();

        return part;
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
}
