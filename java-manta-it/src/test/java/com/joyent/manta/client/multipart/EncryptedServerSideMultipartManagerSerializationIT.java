/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.FastInput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import com.joyent.manta.exception.MantaClientException;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaErrorCode;
import com.joyent.manta.exception.MantaMultipartException;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.serialization.EncryptionStateSerializer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import javax.crypto.Cipher;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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

        kryo.register(EncryptionStateSerializer.class, new EncryptionStateSerializer(kryo));
    }

    @AfterClass
    public void afterClass() throws IOException {
        if (mantaClient != null) {
            mantaClient.deleteRecursive(testPathPrefix);
            mantaClient.closeWithWarning();
        }
    }

    public final void canResumeUploadWithByteArrayAndMultipleParts() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final byte[] content = RandomUtils.nextBytes(FIVE_MB + 1024);
        final byte[] content1 = Arrays.copyOfRange(content, 0, FIVE_MB + 1);
        final byte[] content2 = Arrays.copyOfRange(content, FIVE_MB + 1, FIVE_MB + 1024);

        String contentType = "application/something-never-seen-before; charset=UTF-8";
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setContentType(contentType);

        EncryptedMultipartUpload<ServerSideMultipartUpload> upload = multipart.initiateUpload(path, null, headers);
        MantaMultipartUploadPart part1 = multipart.uploadPart(upload, 1, content1);

        final byte[] serializedEncryptionState;

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
             Output output = new Output(outputStream, 1_000_000)) {

            EncryptionStateSerializer serializer = new EncryptionStateSerializer(kryo);
            serializer.write(kryo, output, upload.getEncryptionState());
            serializedEncryptionState = outputStream.toByteArray();
        }

        UUID resumeUploadId = upload.getId();
        String resumePath = upload.getPath();
        String resumePartsDirectory = upload.getWrapped().getPartsDirectory();


        MantaMultipartUploadPart part2 = resumeUpload(serializedEncryptionState, resumeUploadId, resumePath, resumePartsDirectory, content2);
        MantaMultipartUploadTuple[] parts = new MantaMultipartUploadTuple[] { part1, part2 };
        Stream<MantaMultipartUploadTuple> partsStream = Arrays.stream(parts);


        multipart.complete(upload, partsStream);

        try (MantaObjectInputStream in = mantaClient.getAsInputStream(path);
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            IOUtils.copy(in, out);

            AssertJUnit.assertArrayEquals("Uploaded multipart data doesn't equal actual object data",
                    content, out.toByteArray());

            Assert.assertEquals(in.getContentType(), contentType,
                    "Set content-type doesn't match actual content type");
        }
    }

    public final MantaMultipartUploadPart resumeUpload(byte[] serializedEncryptionState, UUID resumeUploadId, String resumePath, String resumePartsDirectory, byte[] content) throws IOException {
        MantaClient resumeClient = new MantaClient(this.config);
        EncryptedServerSideMultipartManager resumeMultipart = new EncryptedServerSideMultipartManager(resumeClient);

        try (Input inputEncryptionState = new FastInput(serializedEncryptionState)) {
            EncryptionStateSerializer serializer = new EncryptionStateSerializer(kryo);
            EncryptionState encryptionState = serializer.read(kryo, inputEncryptionState, EncryptionState.class);
            ServerSideMultipartUpload serverSideMultipartUpload = new ServerSideMultipartUpload(resumeUploadId, resumePath, resumePartsDirectory);

            EncryptedMultipartUpload<ServerSideMultipartUpload> resumeUpload = new EncryptedMultipartUpload<>(serverSideMultipartUpload, encryptionState);

            MantaMultipartUploadPart part = resumeMultipart.uploadPart(resumeUpload, 2, content);

            return part;
        }
    }
}
