package com.joyent.manta.client.multipart;

import com.joyent.manta.client.MantaClient;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;


// SHHHH IT IS  A SECRET
public class EncryptingMantaMultipartManager extends MantaMultipartManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(EncryptingMantaMultipartManager.class);

    // need one part for hmac
    public static final int MAX_PARTS = MantaMultipartManager.MAX_PARTS - 1;

    //private final ConcurrentHashMap<MantaMultipartUpload,EncryptionContextxb> upload2context;


    public EncryptingMantaMultipartManager(final MantaClient mantaClient) {
        super(mantaClient);
        Validate.isTrue(mantaClient.getContext().isClientEncryptionEnabled(),
                        "MantaClient for encrypted multipart upload has have encryption enabled");
        //upload2context = new ConcurrentHashMap<>();
    }


    //    private static class MultipartEncryptionContext {

        //cipher or entity junk? Context?
        // iostream
        // lock!?!
    //    }
}
