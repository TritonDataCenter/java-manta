/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.joyent.manta.config.IntegrationTestConfigContext;
import com.joyent.manta.config.ConfigContext;
import org.testng.Assert;
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
        testPathPrefix = String.format("%s/stor/%s/",
                config.getMantaHomeDirectory(), UUID.randomUUID());
        mantaClient.putDirectory(testPathPrefix, null);
    }


    @AfterClass
    public void cleanup() throws IOException {
        if (mantaClient != null) {
            mantaClient.deleteRecursive(testPathPrefix);
            mantaClient.closeWithWarning();
        }
    }


    @Test( groups = { "metadata" })
    public final void verifyMetadataIsAddedOnPut() throws IOException {
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
    }

    @Test( groups = { "metadata" })
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

        final MantaObject head = mantaClient.head(path);
        MantaMetadata actualMetadata = head.getMetadata();

        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            String key = entry.getKey();
            String val = entry.getValue();
            Assert.assertEquals(actualMetadata.get(key), val);
        }
    }

    @Test( groups = { "metadata" })
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
    }

    @Test(groups = { "metadata", "directory" })
    public void canAddMetadataToDirectory() throws IOException {
        String dir = String.format("%s/%s", testPathPrefix, UUID.randomUUID());
        mantaClient.putDirectory(dir);

        MantaMetadata metadata = new MantaMetadata();
        metadata.put("m-test", "value");

        mantaClient.putMetadata(dir, metadata);

        MantaObject head = mantaClient.head(dir);
        MantaMetadata remoteMetadata = head.getMetadata();

        Assert.assertTrue(remoteMetadata.containsKey("m-test"));
        Assert.assertEquals(metadata.get("m-test"), remoteMetadata.get("m-test"),
                "Set metadata doesn't equal actual metadata");
    }
}
