package com.joyent.manta.client;

import com.joyent.manta.client.helper.IntegrationTestHelper;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import org.apache.commons.text.RandomStringGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Tests for verifying correct functioning of making remote requests i.e GET bucket(s)/object(s))
 * pertinent to list operations against Manta buckets.
 * <p>
 * Ensure that the testType parameter before running this class is configured to the value "buckets"
 * or else it will throw a {@link org.testng.SkipException}
 * </p>
 */
@SuppressWarnings("unused")
public class MantaBucketListingIteratorIT {

    private static final Logger LOG = LoggerFactory.getLogger(MantaBucketListingIteratorIT.class);

    private static final RandomStringGenerator STRING_GENERATOR =
            new RandomStringGenerator.Builder()
                    .withinRange((int) 'a', (int) 'z')
                    .build();

    private static final String TEST_DATA = "I am Buckethead. We are Buckethead. We are Legion. " +
            "Does that answer your question?";

    private MantaClient mantaClient;

    private String testPathPrefix;

    @BeforeClass
    @Parameters({"usingEncryption", "testType"})
    public void beforeClass(@Optional Boolean usingEncryption,
                            @Optional String testType) throws IOException {
        if (!"buckets".equals(testType)) {
            throw new SkipException("Bucket tests will be skipped in Manta Directories");
        }

        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new IntegrationTestConfigContext(usingEncryption);
        final String testName = this.getClass().getSimpleName();

        mantaClient = new MantaClient(config);
        testPathPrefix = IntegrationTestHelper.setupTestPath(config, mantaClient,
                testName, testType);

        IntegrationTestHelper.createTestBucketOrDirectory(mantaClient, testPathPrefix, testType);
    }

    @AfterClass
    public void afterClass() throws IOException {
        IntegrationTestHelper.cleanupTestBucketOrDirectory(mantaClient, testPathPrefix);
    }

    @Test
    public final void testBucketListing() throws IOException {
        mantaClient.put(String.format("%s%s", testPathPrefix, UUID.randomUUID()), TEST_DATA);

        final String pathPrefix = testPathPrefix.substring(0, testPathPrefix.lastIndexOf(MantaClient.SEPARATOR));
        final Stream<MantaObject> objs = mantaClient.listBucketObjects(pathPrefix);

        final AtomicInteger count = new AtomicInteger(0);
        objs.forEach(obj -> {
            count.incrementAndGet();
            Assert.assertTrue(obj.getPath().startsWith(testPathPrefix));
        });

        Assert.assertEquals(1, count.get());
    }
}
