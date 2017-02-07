package com.joyent.manta.client.multipart;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.client.crypto.EncryptingEntityHelper;
import com.joyent.manta.client.crypto.EncryptingPartEntity;
import com.joyent.manta.client.crypto.EncryptionContext;
import com.joyent.manta.http.EncryptionHttpHelper;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.util.HmacOutputStream;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.Validate;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

// SHHHH IT IS  A SECRET
// DOC thread safety, init vs put and locking
public class EncryptingJobsMultipartManager extends JobsMultipartManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(EncryptingMultipartManager.class);

    // need one part for hmac
    public static final int MAX_PARTS = 10_000 - 1;


    private final EncryptionHttpHelper httpHelper;
    private final ConcurrentHashMap<MantaMultipartUpload,EncryptionState> uploadState;


    public EncryptingJobsMultipartManager(final MantaClient mantaClient) {
        super(mantaClient);
        Validate.isTrue(mantaClient.getContext().isClientEncryptionEnabled(),
                "MantaClient for encrypted multipart upload has have encryption enabled");
        this.httpHelper = (EncryptionHttpHelper)this.mantaClient.httpHelper;
        uploadState = new ConcurrentHashMap<>();
    }

    @Override
    public MantaMultipartUpload initiateUpload(final String path,
                                               final MantaMetadata mantaMetadata,
                                               final MantaHttpHeaders httpHeaders) throws IOException {
        return initiateUpload(path, null, mantaMetadata, httpHeaders);
    }


    @Override
    public MantaMultipartUpload initiateUpload(final String path,
                                               final Long contentLength,
                                               final MantaMetadata mantaMetadata,
                                               final MantaHttpHeaders httpHeaders) throws IOException {
        final MantaMetadata metadata;

        if (mantaMetadata == null) {
            metadata = new MantaMetadata();
        } else {
            metadata = mantaMetadata;
        }

        EncryptionContext eContext = this.httpHelper.newEncryptionContext();
        // last init wins?

        httpHelper.attachEncryptionCipherHeaders(metadata);
        httpHelper.attachEncryptedMetadata(metadata);
        httpHelper.attachEncryptedEntityHeaders(metadata, eContext.getCipher());
        /* We remove the e- prefixed metadata because we have already
         * encrypted it and stored it in m-encrypt-metadata. */
        metadata.removeAllEncrypted();

        /* If content-length is specified, we store the content length as
         * the plaintext content length on the object's metadata and then
         * remove the header because server-side MPU will attempt to verify
         * that the content-length in the header is the same as the ciphertext's
         * length. This won't work because the ciphertext length will always
         * be different than the plaintext content length. */
        if (httpHeaders.getContentLength() != null && httpHeaders.getContentLength() > -1) {
            metadata.put(MantaHttpHeaders.ENCRYPTION_PLAINTEXT_CONTENT_LENGTH,
                    httpHeaders.getContentLength().toString());
            httpHeaders.remove(HttpHeaders.CONTENT_LENGTH);
        } else if (contentLength != null && contentLength > -1) {
            metadata.put(MantaHttpHeaders.ENCRYPTION_PLAINTEXT_CONTENT_LENGTH,
                    contentLength.toString());
        }

        /* If the Content-MD5 header is set, it will always be inaccurate because
         * the ciphertext will have a different checksum and it will cause the
         * server-side checksum verification to fail. */
        if (httpHeaders.getContentMD5() != null) {
            httpHeaders.remove(HttpHeaders.CONTENT_MD5);
        }

        MantaMultipartUpload upload = super.initiateUpload(path, contentLength,
                metadata, httpHeaders);
        uploadState.put(upload, new EncryptionState(eContext));
        return upload;
    }

    @Override
    public MantaMultipartUploadPart uploadPart(MantaMultipartUpload upload,
                                               int partNumber,
                                               String contents) throws IOException {
        byte[] stringContents = contents.getBytes("UTF-8");
        try (ByteArrayInputStream bin = new ByteArrayInputStream(stringContents)) {
            return uploadPart(upload, partNumber, bin);
        }
    }

    @Override
    public MantaMultipartUploadPart uploadPart(MantaMultipartUpload upload, int partNumber, byte[] bytes) throws IOException {
        try (ByteArrayInputStream bin = new ByteArrayInputStream(bytes)) {
            return uploadPart(upload, partNumber, bin);
        }
    }

    @Override
    public MantaMultipartUploadPart uploadPart(MantaMultipartUpload upload, int partNumber, long contentLength, InputStream inputStream) throws IOException {
        return super.uploadPart(upload, partNumber, inputStream);
    }

    @Override
    public MantaMultipartUploadPart uploadPart(MantaMultipartUpload upload, int partNumber, File file) throws IOException {
        try (FileInputStream fin = new FileInputStream(file)) {
            return uploadPart(upload, partNumber, fin);
        }
    }

    public MantaMultipartUploadPart uploadPart(final MantaMultipartUpload upload,
                                               final int partNumber,
                                               final InputStream inputStream)
            throws IOException {
        Validate.notNull(upload, "Multipart upload object must not be null");
        EncryptionState eState = uploadState.get(upload);
        eState.lock.lock();
        try {
            validatePartNumber(partNumber);
            if (eState.lastPartNumber != -1) {
                Validate.isTrue(eState.lastPartNumber + 1 == partNumber,
                        "Encrypted MPU parts must be serial and sequantial");
            } else {
                eState.multipartStream = new MultipartOutputStream(eState.eContext.getCipherDetails().getBlockSizeInBytes());
                eState.cipherStream = EncryptingEntityHelper.makeCipherOutputforStream(eState.multipartStream, eState.eContext);
            }
            final String path = multipartPath(upload.getId(), partNumber);

            // The content type of the individual parts is unimportant
            // to the job that produced the final result
            ContentType contentType = ContentType.APPLICATION_OCTET_STREAM;
            EncryptingPartEntity entity = new EncryptingPartEntity(
                    eState.cipherStream, eState.multipartStream,
                    new InputStreamEntity(inputStream, contentType));
            final MantaObjectResponse response = ((EncryptionHttpHelper) mantaClient.httpHelper).rawHttpPut(path, null, entity, null);
            eState.lastPartNumber = partNumber;
            return new MantaMultipartUploadPart(response);
        } finally {
            eState.lock.unlock();
        }
    }

    // TODO: why is this necessary?
    public void complete(final MantaMultipartUpload upload,
                         final Iterable<? extends MantaMultipartUploadTuple> parts)
            throws IOException {
        try (Stream<? extends MantaMultipartUploadTuple> stream =
                     StreamSupport.stream(parts.spliterator(), false)) {
            complete(upload, stream);
        }
    }


    public void complete(final MantaMultipartUpload upload,
                         final Stream<? extends MantaMultipartUploadTuple> partsStream) throws IOException {
        EncryptionState eState = uploadState.get(upload);
        eState.lock.lock();
        try {
            Stream<? extends MantaMultipartUploadTuple> finalPartsStream = partsStream;
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            eState.multipartStream.setNext(baos);
            eState.cipherStream.close();
            baos.write(eState.multipartStream.getRemainder());

            // conditionally get hmac and upload part; yeah reenterant lock
            if (eState.cipherStream instanceof HmacOutputStream) {
                byte[] hmacBytes = ((HmacOutputStream) eState.cipherStream).getHmac().doFinal();
                Validate.isTrue(hmacBytes.length == eState.eContext.getCipherDetails().getAuthenticationTagOrHmacLengthInBytes(),
                        "HMAC actual bytes doesn't equal the number of bytes expected");

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("HMAC: {}", Hex.encodeHexString(hmacBytes));
                }
                baos.write(hmacBytes);
            }
            if (baos.size() > 0) {
                ByteArrayEntity entity = new ByteArrayEntity(baos.toByteArray());
                final String path = multipartPath(upload.getId(), eState.lastPartNumber+1);
                final MantaObjectResponse response = ((EncryptionHttpHelper) mantaClient.httpHelper).rawHttpPut(path, null, entity, null);
                MantaMultipartUploadPart finalPart = new MantaMultipartUploadPart(response);
                finalPartsStream = Stream.concat(partsStream, Stream.of(finalPart));
            }
            final MantaMetadata encryptionMetadata = new MantaMetadata();
            ((EncryptionHttpHelper) mantaClient.httpHelper).attachEncryptionCipherHeaders(encryptionMetadata);
            ((EncryptionHttpHelper) mantaClient.httpHelper).attachEncryptedEntityHeaders(encryptionMetadata, eState.eContext.getCipher());
            //((EncryptionHttpHelper) mantaClient.httpHelper).attachEncryptionPlaintextLengthHeader(metadata, eContext.getCipher());

//            if (eState.mantaMetadata != null) {
//                MantaMetadata encryptedMetadata = new MantaMetadata(eState.mantaMetadata);
//                ((EncryptionHttpHelper) mantaClient.httpHelper).attachEncryptedMetadata(encryptedMetadata);
//                encryptionMetadata.putAll(encryptedMetadata);
//            }

            super.complete(upload, finalPartsStream);
        } finally {
            eState.lock.unlock();
        }
        uploadState.remove(upload);
    }
}
