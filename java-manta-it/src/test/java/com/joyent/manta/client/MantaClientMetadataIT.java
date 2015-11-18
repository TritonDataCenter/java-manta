/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.joyent.manta.client.config.TestConfigContext;
import com.joyent.manta.config.ConfigContext;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
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
    @Parameters({"manta.url", "manta.user", "manta.key_path", "manta.key_id", "manta.timeout"})
    public void beforeClass(@Optional String mantaUrl,
                            @Optional String mantaUser,
                            @Optional String mantaKeyPath,
                            @Optional String mantaKeyId,
                            @Optional Integer mantaTimeout)
            throws IOException {

        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new TestConfigContext(
                mantaUrl, mantaUser, mantaKeyPath, mantaKeyId, mantaTimeout);

        mantaClient = new MantaClient(config);
        testPathPrefix = String.format("/%s/stor/%s/", config.getMantaUser(), UUID.randomUUID());
        mantaClient.putDirectory(testPathPrefix, null);
    }


    @AfterClass
    public void cleanup() throws IOException {
        mantaClient.deleteRecursive(testPathPrefix);
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

        Assert.assertEquals(metadata, result.getMetadata());

        final MantaObject head = mantaClient.head(path);
        Assert.assertEquals(head.getHeaderAsString("m-Yoda"), "Master");
        Assert.assertEquals(head.getHeaderAsString("m-Droids"), "1");
        Assert.assertEquals(head.getHeaderAsString("m-force"), "true");

        Assert.assertEquals(metadata, head.getMetadata());

        mantaClient.delete(path);
    }

    @Test( groups = { "metadata" })
    public final void verifyMetadataCanBeAddedLater() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        final MantaObject result = mantaClient.put(path, TEST_DATA);
        Assert.assertTrue(result.getMetadata().isEmpty());

        MantaMetadata metadata = new MantaMetadata();
        metadata.put("m-Yoda", "Master");
        metadata.put("m-Droids", "1");
        metadata.put("m-force", "true");

        final MantaObject metadataResult = mantaClient.putMetadata(path, metadata);
        Assert.assertEquals(metadataResult.getMetadata(), metadata);

        final MantaObject head = mantaClient.head(path);
        Assert.assertEquals(metadata, head.getMetadata());

        mantaClient.delete(path);
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

        @SuppressWarnings("unchecked")
        MantaMetadata cleared = (MantaMetadata)metadata.clone();
        cleared.remove("m-force");

        MantaObject head = mantaClient.head(path);
        Assert.assertEquals(cleared, head.getMetadata());

        mantaClient.delete(path);
    }
}
