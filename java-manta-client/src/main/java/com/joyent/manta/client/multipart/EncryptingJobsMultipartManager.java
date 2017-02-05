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
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
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


    public MantaMultipartUpload initiateUpload(final String path,
                                               final MantaMetadata mantaMetadata,
                                               final MantaHttpHeaders httpHeaders) throws IOException {
        MantaMultipartUpload upload = super.initiateUpload(path, null, httpHeaders);
        EncryptionContext eContext = this.httpHelper.newEncryptionContext();
        // last init wins?
        uploadState.put(upload, new EncryptionState(eContext, mantaMetadata));
        return upload;
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
            EncryptingPartEntity entity = new EncryptingPartEntity(eState.eContext, eState.cipherStream, eState.multipartStream,
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

            if (eState.mantaMetadata != null) {
                MantaMetadata encryptedMetadata = new MantaMetadata(eState.mantaMetadata);
                ((EncryptionHttpHelper) mantaClient.httpHelper).attachEncryptedMetadata(encryptedMetadata);
                encryptionMetadata.putAll(encryptedMetadata);
            }

            super.complete(upload, finalPartsStream, encryptionMetadata);
        } finally {
            eState.lock.unlock();
        }
        uploadState.remove(upload);
    }



    private static class EncryptionState {
        // FIXME with public stuff
        public final EncryptionContext eContext;
        public final ReentrantLock lock;
        public int lastPartNumber = -1;
        public MultipartOutputStream multipartStream = null;
        public OutputStream cipherStream = null;
        // If we are going to state, it is easy enough to keep metadata, and this helps us encrypt in the right place
        public MantaMetadata mantaMetadata = null;
        //cipher or entity junk? Context?
        // iostream
        // lock!?!
        public EncryptionState(EncryptionContext eContext, MantaMetadata mantaMetadata) {
            this.eContext = eContext;
            this.lock = new ReentrantLock();
            this.mantaMetadata = mantaMetadata;
        }

    }
}
