package com.joyent.manta.client.multipart;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.exception.MantaClientEncryptionException;
import com.joyent.manta.exception.MantaMultipartException;
import com.joyent.manta.http.entity.MantaInputStreamEntity;
import com.joyent.manta.util.HmacInputStream;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.lang3.Validate;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.bouncycastle.jcajce.io.CipherInputStream;
import org.slf4j.Logger;
import org.apache.http.entity.InputStreamEntity;
import org.slf4j.LoggerFactory;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.http.EncryptionHttpHelper;
import com.joyent.manta.client.crypto.EncryptionContext;
import org.apache.commons.codec.binary.Hex;
import com.joyent.manta.client.crypto.EncryptingEntityHelper;
import com.joyent.manta.util.HmacOutputStream;
import com.joyent.manta.client.crypto.EncryptingPartEntity;
import org.apache.http.entity.ContentType;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import java.io.*;
import java.security.GeneralSecurityException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.spec.AlgorithmParameterSpec;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

// SHHHH IT IS  A SECRET
// DOC thread safety, init vs put and locking
public class EncryptingMantaMultipartManager
        <WRAPPED_MANAGER extends MantaMultipartManager,
         WRAPPED_UPLOAD extends MantaMultipartUpload>
        implements MantaMultipartManager<EncryptedMultipartUpload<WRAPPED_UPLOAD>,
                                         MantaMultipartUploadPart> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EncryptingMantaMultipartManager.class);

    // need one part for hmac
    public static final int MAX_PARTS = MantaMultipartManager.MAX_PARTS - 1;

    private final SupportedCipherDetails cipherDetails;

    private final EncryptionHttpHelper httpHelper;
    private final MantaClient mantaClient;
    private final WRAPPED_MANAGER wrapped;
    private final SecretKey secretKey;

    public EncryptingMantaMultipartManager(final SecretKey secretKey,
                                           final MantaClient mantaClient,
                                           final WRAPPED_MANAGER wrapped) {
        Validate.isTrue(mantaClient.getContext().isClientEncryptionEnabled(),
                        "MantaClient for encrypted multipart upload has have encryption enabled");
        this.secretKey = secretKey;
        this.mantaClient = mantaClient;
        this.wrapped = wrapped;
        this.httpHelper = (EncryptionHttpHelper)this.mantaClient.httpHelper;
        this.cipherDetails = this.httpHelper.getCipherDetails();
    }

    @Override
    public EncryptedMultipartUpload<WRAPPED_UPLOAD> initiateUpload(String path) throws IOException {
        return initiateUpload(path, null);
    }

    @Override
    public EncryptedMultipartUpload<WRAPPED_UPLOAD> initiateUpload(String path, MantaMetadata mantaMetadata) throws IOException {
        return initiateUpload(path, mantaMetadata, null);
    }

    @Override
    public EncryptedMultipartUpload<WRAPPED_UPLOAD> initiateUpload(final String path,
                                                                   final MantaMetadata mantaMetadata,
                                                                   final MantaHttpHeaders httpHeaders) throws IOException {
        MantaMultipartUpload upload = wrapped.initiateUpload(path, mantaMetadata, httpHeaders);

        EncryptionContext eContext = this.httpHelper.newEncryptionContext();

        final Cipher cipher = cipherDetails.getCipher();
        final byte[] iv = cipher.getIV();

        try {
            AlgorithmParameterSpec spec = cipherDetails.getEncryptionParameterSpec(iv);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, spec);
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
                hmac.init(secretKey);
            } catch (InvalidKeyException e) {
                String msg = "Couldn't initialize HMAC with secret key";
                throw new MantaClientEncryptionException(msg, e);
            }

            hmac.update(iv);
        }

        @SuppressWarnings("unchecked")
        EncryptedMultipartUpload<WRAPPED_UPLOAD> encryptedUpload =
                new EncryptedMultipartUpload(upload, cipher, hmac);

        // TODO: Figure out how to embed metadata / headers

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
    @SuppressWarnings("unchecked")
    public void complete(final EncryptedMultipartUpload<WRAPPED_UPLOAD> upload,
                         final Stream<? extends MantaMultipartUploadTuple> partsStream)
            throws IOException {
        final Stream<? extends MantaMultipartUploadTuple> finalPartsStream;

        if (cipherDetails.isAEADCipher()) {
            finalPartsStream = partsStream;
        } else {
            final byte[] hmac = upload.getHmac().doFinal();
            MantaMultipartUploadPart hmacPart = uploadPart(
                    upload, upload.getCurrentPartNumber() + 1, hmac);
            finalPartsStream = Stream.concat(partsStream, Stream.of(hmacPart));
        }

        wrapped.complete(upload, finalPartsStream);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Stream<MantaMultipartUpload> listInProgress() throws IOException {
        return wrapped.listInProgress();
    }

    @Override
    @SuppressWarnings("unchecked")
    public MantaMultipartUploadPart uploadPart(final EncryptedMultipartUpload<WRAPPED_UPLOAD> upload,
                                               final int partNumber,
                                               final String contents) throws IOException {
        InputStream in = new ByteArrayInputStream(contents.getBytes(Charsets.UTF_8));
        return uploadPart(upload, partNumber, in);
    }

    @Override
    @SuppressWarnings("unchecked")
    public MantaMultipartUploadPart uploadPart(final EncryptedMultipartUpload<WRAPPED_UPLOAD> upload,
                                               final int partNumber,
                                               final byte[] bytes) throws IOException {
        InputStream in = new ByteArrayInputStream(bytes);
        return uploadPart(upload, partNumber, in);
    }

    @Override
    @SuppressWarnings("unchecked")
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

        if (upload.getLastBlockOverrunBytes().length > 0) {

        }

        // TODO: Add BoundedInputStream and store remainder

        if (cipherDetails.isAEADCipher()) {
            in = new CipherInputStream(inputStream, upload.getCipher());
        } else {
            CipherInputStream cipherStream = new CipherInputStream(inputStream, upload.getCipher());
            in = new HmacInputStream(upload.getHmac(), cipherStream);
        }

        @SuppressWarnings("unchecked")
        MantaMultipartUploadPart part = wrapped.uploadPart(upload, partNumber, in);

        upload.incrementPartNumber();

        return part;
    }

    @Override
    @SuppressWarnings("unchecked")
    public MantaMultipartUploadPart getPart(EncryptedMultipartUpload<WRAPPED_UPLOAD> upload, int partNumber) throws IOException {
        return wrapped.getPart(upload, partNumber);
    }

    @Override
    @SuppressWarnings("unchecked")
    public MantaMultipartStatus getStatus(EncryptedMultipartUpload<WRAPPED_UPLOAD> upload) throws IOException {
        return wrapped.getStatus(upload);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Stream<MantaMultipartUploadPart> listParts(EncryptedMultipartUpload<WRAPPED_UPLOAD> upload)
            throws IOException {
        return wrapped.listParts(upload);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void validateThatThereAreSequentialPartNumbers(EncryptedMultipartUpload<WRAPPED_UPLOAD> upload) throws IOException, MantaMultipartException {
        wrapped.validateThatThereAreSequentialPartNumbers(upload);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void abort(EncryptedMultipartUpload<WRAPPED_UPLOAD> upload) throws IOException {
        wrapped.abort(upload);
    }
}
