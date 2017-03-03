/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import com.esotericsoftware.kryo.Kryo;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.crypto.SupportedCiphersLookupMap;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.serialization.SerializationHelper;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Stream;

import static org.testng.Assert.fail;

@Test(groups = { "encrypted" })
@SuppressWarnings("Duplicates")
public class EncryptedServerSideMultipartManagerSerializationIT {
    private MantaClient mantaClient;
    private EncryptedServerSideMultipartManager multipart;

    private static final int FIVE_MB = 5242880;

    private String testPathPrefix;

    private Kryo kryo = new Kryo();

    private ConfigContext config;

    @BeforeClass()
    @Parameters({"usingEncryption"})
    public void beforeClass(@org.testng.annotations.Optional Boolean usingEncryption) throws IOException {
        // Let TestNG configuration take precedence over environment variables
        this.config = new IntegrationTestConfigContext(usingEncryption);

        if (!config.isClientEncryptionEnabled()) {
            throw new SkipException("Skipping tests if encryption is disabled");
        }
        mantaClient = new MantaClient(config);

        if (!mantaClient.existsAndIsAccessible(config.getMantaHomeDirectory()
                + MantaClient.SEPARATOR + "uploads")) {
            throw new SkipException("Server side uploads aren't supported in this Manta version");
        }

        multipart = new EncryptedServerSideMultipartManager(this.mantaClient);
        testPathPrefix = String.format("%s/stor/java-manta-integration-tests/%s",
                config.getMantaHomeDirectory(), UUID.randomUUID());
        mantaClient.putDirectory(testPathPrefix, true);
    }

    @AfterClass
    public void afterClass() throws IOException {
        if (mantaClient != null) {
            mantaClient.deleteRecursive(testPathPrefix);
            mantaClient.closeWithWarning();
        }
    }

    public final void canResumeUploadWithByteArrayAndMultipleParts() throws Exception {
        final SupportedCipherDetails cipherDetails = SupportedCiphersLookupMap.INSTANCE.get(
                config.getEncryptionAlgorithm());
        final SecretKey secretKey = SecretKeyUtils.loadKey(config.getEncryptionPrivateKeyBytes(),
                cipherDetails);
        final SerializationHelper<ServerSideMultipartUpload> helper =
                new SerializationHelper<>(kryo, secretKey, cipherDetails, ServerSideMultipartUpload.class);
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final byte[] content = new byte[FIVE_MB + 1024];
        Arrays.fill(content, (byte)'f');
        final byte[] content1 = Arrays.copyOfRange(content, 0, FIVE_MB + 1);
        final byte[] content2 = Arrays.copyOfRange(content, FIVE_MB + 1, FIVE_MB + 1024);

        String contentType = "application/something-never-seen-before; charset=UTF-8";
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setContentType(contentType);

        EncryptedMultipartUpload<ServerSideMultipartUpload> upload = multipart.initiateUpload(path, null, headers);
        MantaMultipartUploadPart part1 = multipart.uploadPart(upload, 1, content1);

        final byte[] serializedEncryptionState = helper.serialize(upload);

        EncryptedMultipartUpload<ServerSideMultipartUpload> deserializedUpload
                = helper.deserialize(serializedEncryptionState);

//        assertReflectionEquals("Cipher state differs between uploads",
//                upload.getEncryptionState().getEncryptionContext().getCipher(),
//                deserializedUpload.getEncryptionState().getEncryptionContext().getCipher());
//
//        Field innerOutputStreamField = FieldUtils.getField(
//                HmacOutputStream.class, "out", true);
//
//        CipherOutputStream uploadCipherStream = (CipherOutputStream)
//                FieldUtils.readField(innerOutputStreamField, upload.getEncryptionState().getCipherStream(), true);
//
//        CipherOutputStream deserializedCipherStream = (CipherOutputStream)
//                FieldUtils.readField(innerOutputStreamField, deserializedUpload.getEncryptionState().getCipherStream(), true);
//
//        Field cipherField = FieldUtils.getField(CipherOutputStream.class, "cipher", true);
//
//        assertReflectionEquals(
//                deserializedUpload.getEncryptionState().getEncryptionContext().getCipher(),
//                FieldUtils.readField(cipherField, deserializedCipherStream, true));

        MantaMultipartUploadPart part2 = multipart.uploadPart(deserializedUpload, 2, content2);
        MantaMultipartUploadTuple[] parts = new MantaMultipartUploadTuple[] { part1, part2 };
        Stream<MantaMultipartUploadTuple> partsStream = Arrays.stream(parts);

        multipart.complete(deserializedUpload, partsStream);

        final byte[] bytes;
        try (MantaObjectInputStream in = mantaClient.getAsInputStream(path)) {
            Assert.assertEquals(in.getContentType(), contentType,
                    "Set content-type doesn't match actual content type");

            int b;
            int i = 0;
            while ((b = in.read()) != -1) {
                final byte expected = content[i++];

                Assert.assertEquals((byte)b, expected,
                        "Byte not matched at position: " + (i - 1));
            }

            if (i + 1 < content.length) {
                fail("Missing " + (content.length - i + 1) + "bytes from Manta stream");
            }
        }
    }
}
