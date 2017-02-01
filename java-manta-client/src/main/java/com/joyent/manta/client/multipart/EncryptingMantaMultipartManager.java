package com.joyent.manta.client.multipart;

import com.joyent.manta.client.MantaClient;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.apache.http.entity.InputStreamEntity;
import org.slf4j.LoggerFactory;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.client.MantaObjectMapper;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.util.MantaUtils;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaException;
import com.joyent.manta.exception.MantaIOException;
import com.joyent.manta.exception.MantaMultipartException;
import com.joyent.manta.http.EncryptionHttpHelper;
import com.joyent.manta.client.crypto.EncryptionContext;
import org.apache.commons.codec.binary.Hex;
import com.joyent.manta.client.crypto.EncryptingEntityHelper;
import com.joyent.manta.util.HmacOutputStream;
import com.joyent.manta.client.crypto.EncryptingPartEntity;
import org.apache.http.entity.ContentType;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.crypto.CipherOutputStream;

// SHHHH IT IS  A SECRET
// DOC thread safety, init vs put and locking
public class EncryptingMantaMultipartManager extends JobsMultipartManager {

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
                eState.multipartStream = new MultipartOutputStream(null);
                eState.cipherStream = EncryptingEntityHelper.makeCipherOutputforStream(eState.multipartStream, eState.eContext);
            }
            final String path = multipartPath(upload.getId(), partNumber);

            //final ContentType contentType = ContentTypeLookup.findOrDefaultContentType(headers, ContentType.APPLICATION_OCTET_STREAM);
            ContentType contentType = ContentType.APPLICATION_OCTET_STREAM;
            EncryptingPartEntity entity = new EncryptingPartEntity(eState.eContext, eState.multipartStream,
                                                                   new InputStreamEntity(inputStream, contentType));
            //final MantaObjectResponse response = (this.mantaClient.httpHelper.super).httpPut(path, null, entity, null);
            eState.lastPartNumber = partNumber;
            return null;
        } finally {
            eState.lock.unlock();
        }
    }

    public void complete(final MantaMultipartUpload upload,
                         final Stream<? extends MantaMultipartUploadTuple> partsStream) throws IOException {
        EncryptionState eState = uploadState.get(upload);
        eState.lock.lock();
        try {
            // conditionally get hmac and upload part; yeah reenterant lock
            if (eState.cipherStream instanceof HmacOutputStream) {
                byte[] hmacBytes = ((HmacOutputStream) eState.cipherStream).getHmac().doFinal();
                Validate.isTrue(hmacBytes.length == eState.eContext.getCipherDetails().getAuthenticationTagOrHmacLengthInBytes(),
                        "HMAC actual bytes doesn't equal the number of bytes expected");

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("HMAC: {}", Hex.encodeHexString(hmacBytes));
                }
                // TODO: figure out how to actuallu upload a part for hmac and adjust stream
            }
            super.complete(upload, partsStream);
        } finally {
            eState.lock.unlock();
        }
    }



    private static class EncryptionState {
        // FIXME with public stuff
        public final EncryptionContext eContext;
        public final ReentrantLock lock;
        public int lastPartNumber = -1;
        public MultipartOutputStream multipartStream = null;
        public OutputStream cipherStream = null;
        //cipher or entity junk? Context?
        // iostream
        // lock!?!
        public EncryptionState(EncryptionContext eContext) {
            this.eContext = eContext;
            this.lock = new ReentrantLock();
        }

    }
}
