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
        testPathPrefix = String.format(config.getMantaBucketsDirectory() + "%s%s"
                , SEPARATOR, this.getClass().getSimpleName().toLowerCase());
    }

    public void createAndDeleteBucket() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String bucketPath = testPathPrefix + name;
        final boolean finished = mantaClient.createBucket(bucketPath);

        mantaClient.deleteBucket(bucketPath);
        Assert.assertTrue(finished);
        Assert.assertFalse(mantaClient.existsAndIsAccessible(bucketPath));
    }

    public void createBucketStartingWithUpperCase() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String bucketPath = testPathPrefix + name;
        boolean failed = false;

        try {
            final boolean finished = mantaClient.createBucket(bucketPath);
            Assert.assertFalse(finished);
        } catch (MantaClientHttpResponseException e) {
            if (e.getServerCode().equals(INVALID_BUCKET_NAME_ERROR)) {
                failed = true;
            }
        }

        Assert.assertTrue(failed, "bucket-name doesn't comply with server rules");
        mantaClient.deleteBucket(bucketPath);
        Assert.assertFalse(mantaClient.existsAndIsAccessible(bucketPath));
    }

    public void updateNonExistentBucketObjectMetadata() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String bucketPath = testPathPrefix + name;
        final boolean finished = mantaClient.createBucket(bucketPath);
        mantaClient.deleteBucket(bucketPath);

        Assert.assertTrue(finished);
    }
}
