package com.joyent.manta.client;

import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.IOException;
import java.util.UUID;

import static com.joyent.manta.exception.MantaErrorCode.INVALID_BUCKET_NAME_ERROR;
import static com.joyent.manta.client.MantaClient.SEPARATOR;

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
        testPathPrefix = String.format(SEPARATOR + config.getMantaUser() + SEPARATOR
                + "buckets" + SEPARATOR + this.getClass().getSimpleName().toLowerCase());
    }

    public void createAndDeleteBucket() throws IOException {

        final String bucketname = String.format(testPathPrefix + "bucket-100");
        final boolean finished = mantaClient.createBucket(bucketname);
        mantaClient.deleteBucket(bucketname);
        Assert.assertTrue(finished);
        Assert.assertFalse(mantaClient.existsAndIsAccessible(bucketname));
    }

    public void createBucketStartingWithUpperCase() throws IOException {
        final String bucketName = String.format(testPathPrefix + "bucket-%s", UUID.randomUUID());
        boolean failed = false;

        try {
            final boolean finished = mantaClient.createBucket(bucketName);
            Assert.assertFalse(finished);
        } catch (MantaClientHttpResponseException e) {
            if (e.getServerCode().equals(INVALID_BUCKET_NAME_ERROR)) {
                failed = true;
            }
        }

        Assert.assertTrue(failed, "bucket-name doesn't comply with server rules");
        mantaClient.deleteBucket(bucketName);
        Assert.assertFalse(mantaClient.existsAndIsAccessible(bucketName));
    }

    public void updateNonExistentBucketObjectMetadata() throws IOException {

        final String bucketname = String.format("/ashwin.nair/buckets/MantaClientBucketsIT-%s", UUID.randomUUID());
        final boolean finished = mantaClient.createBucket(bucketname);
        mantaClient.deleteBucket(bucketname);

        Assert.assertTrue(finished);
    }
}
