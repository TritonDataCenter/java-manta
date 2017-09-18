/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.config.IntegrationTestConfigContext;
import com.joyent.manta.config.ConfigContext;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;


/**
 * Tests for verifying the behavior of metadata with {@link MantaClient}.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
@Test( groups = { "metadata" })
public class MantaClientMetadataIT {
    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";

    private MantaClient mantaClient;

    private String testPathPrefix;

    @BeforeClass
    @Parameters({"usingEncryption"})
    public void beforeClass(@Optional Boolean usingEncryption) throws IOException {

        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new IntegrationTestConfigContext(usingEncryption);

        mantaClient = new MantaClient(config);
        testPathPrefix = IntegrationTestConfigContext.generateBasePath(config);
        mantaClient.putDirectory(testPathPrefix, true);
    }

    @AfterClass
    public void cleanup() throws IOException {
        if (mantaClient != null) {
            mantaClient.deleteRecursive(testPathPrefix);
            mantaClient.closeWithWarning();
        }
    }

    public final void verifyAddMetadataToObjectOnPut() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        MantaMetadata metadata = new MantaMetadata();
        metadata.put("m-Yoda", "Master");
        metadata.put("m-Droids", "1");
        metadata.put("m-force", "true");

        final MantaObject result = mantaClient.put(path, TEST_DATA, metadata);
        Assert.assertEquals(result.getHeaderAsString("m-Yoda"), "Master");
        Assert.assertEquals(result.getHeaderAsString("m-Droids"), "1");
        Assert.assertEquals(result.getHeaderAsString("m-force"), "true");

        final MantaObject head = mantaClient.head(path);
        Assert.assertEquals(head.getHeaderAsString("m-Yoda"), "Master");
        Assert.assertEquals(head.getHeaderAsString("m-Droids"), "1");
        Assert.assertEquals(head.getHeaderAsString("m-force"), "true");

        final MantaObject get = mantaClient.head(path);
        Assert.assertEquals(get.getHeaderAsString("m-Yoda"), "Master");
        Assert.assertEquals(get.getHeaderAsString("m-Droids"), "1");
        Assert.assertEquals(get.getHeaderAsString("m-force"), "true");
    }

    public final void verifyMetadataCanBeAddedLater() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        final MantaObject result = mantaClient.put(path, TEST_DATA);

        // There will be existing headers if we are in encrypted mode
        if (!mantaClient.getContext().isClientEncryptionEnabled()) {
            Assert.assertTrue(result.getMetadata().isEmpty());
        }

        MantaMetadata metadata = new MantaMetadata();
        metadata.put("m-Yoda", "Master");
        metadata.put("m-Droids", "1");
        metadata.put("m-force", "true");

        final MantaObject metadataResult = mantaClient.putMetadata(path, metadata);

        Assert.assertEquals(metadataResult.getMetadata(), metadata);

        {
            final MantaObject head = mantaClient.head(path);
            MantaMetadata actualMetadata = head.getMetadata();

            for (Map.Entry<String, String> entry : metadata.entrySet()) {
                String key = entry.getKey();
                String val = entry.getValue();
                Assert.assertEquals(actualMetadata.get(key), val);
            }
        }
        {
            final MantaObject get = mantaClient.get(path);
            MantaMetadata actualMetadata = get.getMetadata();

            for (Map.Entry<String, String> entry : metadata.entrySet()) {
                String key = entry.getKey();
                String val = entry.getValue();
                Assert.assertEquals(actualMetadata.get(key), val);
            }
        }
    }

    public final void verifyCanRemoveMetadata() throws IOException, CloneNotSupportedException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        MantaMetadata metadata = new MantaMetadata();
        metadata.put("m-Yoda", "Master");
        metadata.put("m-Droids", "1");
        metadata.put("m-force", "true");

        final MantaObject result = mantaClient.put(path, TEST_DATA, metadata);
        Assert.assertEquals(metadata, result.getMetadata());

        // Assume metadata was added correctly
        @SuppressWarnings("unchecked")
        MantaMetadata updated = (MantaMetadata)metadata.clone();
        updated.delete("m-force");

        MantaObject updateResult = mantaClient.putMetadata(path, updated);
        Assert.assertEquals(updated, updateResult.getMetadata());

        MantaObject head = mantaClient.head(path);
        Assert.assertNull(head.getMetadata().get("m-force"),
                String.format("Actual metadata: %s", head.getMetadata()));

        MantaObject get = mantaClient.get(path);
        Assert.assertNull(get.getMetadata().get("m-force"),
                String.format("Actual metadata: %s", get.getMetadata()));
    }

    @Test(groups = { "metadata", "directory" })
    public void canAddMetadataToDirectory() throws IOException {
        String dir = String.format("%s/%s", testPathPrefix, UUID.randomUUID());
        mantaClient.putDirectory(dir);

        MantaMetadata metadata = new MantaMetadata();
        metadata.put("m-test", "value");

        mantaClient.putMetadata(dir, metadata);

        {
            MantaObject head = mantaClient.head(dir);
            MantaMetadata remoteMetadata = head.getMetadata();

            Assert.assertTrue(remoteMetadata.containsKey("m-test"));
            Assert.assertEquals(metadata.get("m-test"), remoteMetadata.get("m-test"),
                    "Set metadata doesn't equal actual metadata");
        }

        {
            MantaObject get = mantaClient.get(dir);
            MantaMetadata remoteMetadata = get.getMetadata();

            Assert.assertTrue(remoteMetadata.containsKey("m-test"));
            Assert.assertEquals(metadata.get("m-test"), remoteMetadata.get("m-test"),
                    "Set metadata doesn't equal actual metadata");
        }
    }

    @Test(groups = "encrypted")
    public void verifyAddEncryptedMetadataToObjectOnPut() throws IOException {
        if (!mantaClient.getContext().isClientEncryptionEnabled()) {
            throw new SkipException("Test is only relevant when client-side encryption is enabled");
        }

        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        MantaMetadata metadata = new MantaMetadata();
        metadata.put("e-Yoda", "Master");
        metadata.put("e-Droids", "1");
        metadata.put("e-force", "true");

        final MantaObject result = mantaClient.put(path, TEST_DATA, metadata);
        Assert.assertEquals(result.getMetadata().get("e-Yoda"), "Master");
        Assert.assertEquals(result.getMetadata().get("e-Droids"), "1");
        Assert.assertEquals(result.getMetadata().get("e-force"), "true");

        final MantaObject head = mantaClient.head(path);
        Assert.assertEquals(head.getHeaderAsString("e-Yoda"), "Master");
        Assert.assertEquals(head.getHeaderAsString("e-Droids"), "1");
        Assert.assertEquals(head.getHeaderAsString("e-force"), "true");

        final MantaObject get = mantaClient.get(path);
        Assert.assertEquals(get.getHeaderAsString("e-Yoda"), "Master");
        Assert.assertEquals(get.getHeaderAsString("e-Droids"), "1");
        Assert.assertEquals(get.getHeaderAsString("e-force"), "true");
    }

    @Test(groups = "encrypted")
    public final void verifyEncryptedMetadataCanBeAddedLater() throws IOException {
        if (!mantaClient.getContext().isClientEncryptionEnabled()) {
            throw new SkipException("Test is only relevant when client-side encryption is enabled");
        }

        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        final MantaObject result = mantaClient.put(path, TEST_DATA);

        // There will be existing headers if we are in encrypted mode
        if (!mantaClient.getContext().isClientEncryptionEnabled()) {
            Assert.assertTrue(result.getMetadata().isEmpty());
        }

        MantaMetadata metadata = new MantaMetadata();
        metadata.put("e-Yoda", "Master");
        metadata.put("e-Droids", "1");
        metadata.put("e-force", "true");

        final MantaObject metadataResult = mantaClient.putMetadata(path, metadata);

        Assert.assertEquals(metadataResult.getMetadata(), metadata);

        {
            final MantaObject head = mantaClient.head(path);
            MantaMetadata actualMetadata = head.getMetadata();

            for (Map.Entry<String, String> entry : metadata.entrySet()) {
                String key = entry.getKey();
                String val = entry.getValue();
                Assert.assertEquals(actualMetadata.get(key), val);
            }
        }

        {
            final MantaObject get = mantaClient.head(path);
            MantaMetadata actualMetadata = get.getMetadata();

            for (Map.Entry<String, String> entry : metadata.entrySet()) {
                String key = entry.getKey();
                String val = entry.getValue();
                Assert.assertEquals(actualMetadata.get(key), val);
            }
        }
    }
}
