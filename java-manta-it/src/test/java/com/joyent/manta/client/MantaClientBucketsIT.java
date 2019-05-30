package com.joyent.manta.client;

import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.IOException;
import java.util.UUID;

/**
 * Tests for verifying buckets operations of {@link MantaClient}
 *
 * @author <a href="https://github.com/nairashwin952013">Ashwin A Nair</a>
 */
@Test(groups = { "buckets" })
public class MantaClientBucketsIT {
    private MantaClient mantaClient;

    private ConfigContext config;

    private String testPathPrefix;

    @BeforeClass
    @Parameters({"usingEncryption"})
    public void beforeClass(@Optional Boolean usingEncryption) throws IOException {

        // Let TestNG configuration take precedence over environment variables
        config = new IntegrationTestConfigContext(usingEncryption);
        mantaClient = new MantaClient(config);
    }

    public void createBucket() throws IOException {

        final String bucketname = String.format("/ashwin.nair/buckets/MantaClientBucketsIT-%s", UUID.randomUUID());

        final boolean finished = mantaClient.createBucket(bucketname);
        Assert.assertTrue(finished);
    }
}
