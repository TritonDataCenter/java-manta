package com.joyent.manta.client.multipart;

import com.joyent.manta.client.MantaClient;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;


// SHHHH IT IS  A SECRET
// DOC thread safety, init vs put and locking
public class EncryptingMantaMultipartManager extends MantaMultipartManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(EncryptingMantaMultipartManager.class);

    // need one part for hmac
    public static final int MAX_PARTS = MantaMultipartManager.MAX_PARTS - 1;


    private final EncryptionHttpHelper httpHelper;
    private final ConcurrentHashMap<MantaMultipartUpload,EncryptionState> uploadState;


    public EncryptingMantaMultipartManager(final MantaClient mantaClient) {
        super(mantaClient);
        Validate.isTrue(mantaClient.getContext().isClientEncryptionEnabled(),
                        "MantaClient for encrypted multipart upload has have encryption enabled");
        this.httpHelper = (EncryptionHttpHelper)this.mantaClient.httpHelper;
        uploadState = new ConcurrentHashMap<>();
    }


    public MantaMultipartUpload initiateUpload(final String path,
                                               final MantaMetadata mantaMetadata,
                                               final MantaHttpHeaders httpHeaders) throws IOException {
        MantaMultipartUpload upload = super.initiateUpload(path, mantaMetadata, httpHeaders);
        EncryptionContext eContext = this.httpHelper.newEncryptionContext();
        // last init wins?
        uploadState.put(upload, new EncryptionState(eContext));
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
                Validate.isTrue(eState.lastPartNumber + 1 = partNumber,
                                "Encrypted MPU parts must be serial and sequantial");
            } else {
                eState.multipartStream = new MultipartOutputStream(null);
                eSate.cipherStream = EncryptedEntityHelper.makeCipherOutputforStream(estate.outStream);
            }
            final String path = multipartPath(id, partNumber);

            //final ContentType contentType = ContentTypeLookup.findOrDefaultContentType(headers, ContentType.APPLICATION_OCTET_STREAM);
            ContentType contentType = ContentType.APPLICATION_OCTET_STREAM;
            EncryptingPartEntity entity = new EncryptingPartEntity(eContext, eState.multipartStream,
                                                                   new InputStreamEntity(inputStream, contentType));
            final MantaObjectResponse response = mantaClient.httpHelper.super.httpPut(path, null, entity, null);
            eState.lastPartNumber = partNumber;
            return response;
        } finally {
            eState.lock.unlock()
        }
        //return uploadPart(upload.getId(), partNumber, inputStream);
    }

    public void complete(final MantaMultipartUpload upload,
                         final Stream<? extends MantaMultipartUploadTuple> partsStream) {
        EncryptionState eState = uploadState.get(upload);
        eState.lock.lock();
        try {
            // conditionally get hmac and upload part; yeah reenterant lock
            if (eState.cipherStream instanceof HmacOutputStream) {
                byte[] hmacBytes = out.getHmac().doFinal();
                Validate.isTrue(hmacBytes.length == eContext.getCipherDetails().getAuthenticationTagOrHmacLengthInBytes(),
                        "HMAC actual bytes doesn't equal the number of bytes expected");

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("HMAC: {}", Hex.encodeHexString(hmacBytes));
                }
                // TODO: figure out how to actuallu upload a part for hmac and adjust stream
            }
            super.complete(upload, partsStream)
        } finally {
            eState.lock.unlock()
        }
        

    }
    
    

    private static class EncryptionState {
        // FIXME with public stuff
        public final EncryptionContext eContext;
        public final ReentrantLock lock;
        public int lastPartNumber = -1;
        public MultipartOutputStream outStream = null;
        public CipherOutputStream cipherStream = null;
        //cipher or entity junk? Context?
        // iostream
        // lock!?!
        public EncryptionState(EncryptionContext eContext) {
            this.eContext = eContext;
            this.lock = new ReentrantLock();
        }
        
    }
}
