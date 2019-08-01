package com.joyent.manta.client;

import com.joyent.manta.client.helper.IntegrationTestHelper;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import org.apache.commons.lang3.Validate;
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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
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
        final String objectPath = testPathPrefix + UUID.randomUUID();
        final String objectPath1 = testPathPrefix + UUID.randomUUID();
        final String objectPath2 = testPathPrefix + UUID.randomUUID();
        final String objectPath3 = testPathPrefix + UUID.randomUUID();
        final String objectPath4 = testPathPrefix + UUID.randomUUID();

        mantaClient.put(objectPath, TEST_DATA);
        mantaClient.put(objectPath1, TEST_DATA);
        mantaClient.put(objectPath2, TEST_DATA);
        mantaClient.put(objectPath3, TEST_DATA);
        mantaClient.put(objectPath4, TEST_DATA);

        final String bucketObjectsDir = prefixObjectPath();
        final Stream<MantaObject> objs = mantaClient.listBucketObjects(bucketObjectsDir);
        final AtomicInteger count = new AtomicInteger(0);
        objs.forEach(obj -> {
            count.incrementAndGet();
            Assert.assertTrue(obj.getPath().startsWith(testPathPrefix));
        });

        Assert.assertEquals(5, count.get());

        mantaClient.deleteBucket(objectPath);
        mantaClient.deleteBucket(objectPath1);
        mantaClient.deleteBucket(objectPath2);
        mantaClient.deleteBucket(objectPath3);
        mantaClient.deleteBucket(objectPath4);
        Assert.assertFalse(mantaClient.existsAndIsAccessible(objectPath));
        Assert.assertFalse(mantaClient.existsAndIsAccessible(objectPath1));
        Assert.assertFalse(mantaClient.existsAndIsAccessible(objectPath2));
        Assert.assertFalse(mantaClient.existsAndIsAccessible(objectPath3));
        Assert.assertFalse(mantaClient.existsAndIsAccessible(objectPath4));
    }

    public void canFindASingleFile() throws IOException {
        final String filePath = testPathPrefix + UUID.randomUUID();
        final String bucketObjectsDir = prefixObjectPath();

        mantaClient.put(filePath, TEST_DATA, StandardCharsets.UTF_8);

        try (Stream<MantaObject> stream = mantaClient.findBucketObject(bucketObjectsDir)) {
            List<MantaObject> results = stream.collect(Collectors.toList());

            Assert.assertFalse(results.isEmpty(), "At the least one file returned");
            Assert.assertEquals(results.get(0).getPath(), filePath);
            Assert.assertFalse(results.get(0).isBucket());
        }

        mantaClient.deleteBucket(filePath);
        Assert.assertFalse(mantaClient.existsAndIsAccessible(filePath));
    }

    private String prefixObjectPath() {
        final String objectPath = testPathPrefix.substring(0, testPathPrefix.lastIndexOf(MantaClient.SEPARATOR));

        Validate.notNull(objectPath, "Parsing object path failed");
        return objectPath;
    }
}
