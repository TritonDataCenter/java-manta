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
import com.joyent.manta.serialization.EncryptedMultipartUploaSerializationHelper;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.security.Provider;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Stream;
import javax.crypto.SecretKey;

import static org.testng.Assert.fail;

@Test(groups = { "encrypted" })
@SuppressWarnings("Duplicates")
public class EncryptedServerSideMultipartManagerSerializationIT {
    private static final Logger LOGGER = LoggerFactory.getLogger
            (EncryptedServerSideMultipartManagerSerializationIT.class);
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
        testPathPrefix = IntegrationTestConfigContext.generateBasePath(config);
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
        final EncryptedMultipartUploaSerializationHelper<ServerSideMultipartUpload> helper =
                new EncryptedMultipartUploaSerializationHelper<>(kryo, secretKey, cipherDetails, ServerSideMultipartUpload.class);
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

        Provider provider = upload.getEncryptionState().getEncryptionContext().getCipher().getProvider();

        LOGGER.info("Testing serialization with encryption provider: {}", provider.getInfo());

        final byte[] serializedEncryptionState = helper.serialize(upload);

        EncryptedMultipartUpload<ServerSideMultipartUpload> deserializedUpload
                = helper.deserialize(serializedEncryptionState);

        MantaMultipartUploadPart part2 = multipart.uploadPart(deserializedUpload, 2, content2);
        MantaMultipartUploadTuple[] parts = new MantaMultipartUploadTuple[] { part1, part2 };
        Stream<MantaMultipartUploadTuple> partsStream = Arrays.stream(parts);

        multipart.complete(deserializedUpload, partsStream);

        try (MantaObjectInputStream in = mantaClient.getAsInputStream(path)) {
            Assert.assertEquals(in.getContentType(), contentType,
                    "Set content-type doesn't match actual content type");

            int b;
            int i = 0;
            while ((b = in.read()) != -1) {
                final byte expected = content[i++];

                Assert.assertEquals((byte)b, expected,
                        "Byte [" + (char)b + "] not matched at position: " + (i - 1));
            }

            if (i + 1 < content.length) {
                fail("Missing " + (content.length - i + 1) + " bytes from Manta stream");
            }
        }
    }
}
