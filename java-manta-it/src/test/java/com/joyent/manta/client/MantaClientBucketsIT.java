/*
 * Copyright (c) 2019-2020, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import com.joyent.manta.exception.MantaIOException;
import com.joyent.manta.util.MantaUtils;
import com.joyent.test.util.MantaAssert;
import com.joyent.test.util.MantaFunction;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.UUID;

import static com.joyent.manta.client.MantaClient.SEPARATOR;
import static com.joyent.manta.client.helper.IntegrationTestHelper.generateBucketName;
import static com.joyent.manta.exception.MantaErrorCode.BUCKET_EXISTS_ERROR;
import static com.joyent.manta.exception.MantaErrorCode.INVALID_BUCKET_NAME_ERROR;

/**
 * Tests for verifying buckets operations of {@link MantaClient}
 *
 * @author <a href="https://github.com/nairashwin952013">Ashwin A Nair</a>
 */
@Test(groups = { "buckets" })
public class MantaClientBucketsIT {
    private static final String TEST_DATA = "Buckets, how you doing ?";

    private MantaClient mantaClient;

    private ConfigContext config;

    private String testPathPrefix;

    @BeforeClass
    @Parameters({"usingEncryption"})
    public void beforeClass(@Optional Boolean usingEncryption) throws IOException {

        // Let TestNG configuration take precedence over environment variables
        config = new IntegrationTestConfigContext(usingEncryption);
        mantaClient = new MantaClient(config);
        testPathPrefix = String.format(config.getMantaBucketsDirectory() + "%s", SEPARATOR);
    }

    @Test(expectedExceptions = { IllegalArgumentException.class })
    public final void generateBucketNameInNonBucketsDirectory() {
        final String path = "/username/stor/";
        generateBucketName(path);
    }

    @Test
    public void createAndDeleteBucket() throws IOException {
        final String name = generateBucketName(testPathPrefix);
        final String bucketPath = testPathPrefix + name;
        Assert.assertTrue(mantaClient.createBucket(bucketPath));
        mantaClient.deleteBucket(bucketPath);
        Assert.assertFalse(mantaClient.existsAndIsAccessible(bucketPath));
    }

    @Test
    public void canListEmptyBucket() throws IOException {
        final String name = generateBucketName(testPathPrefix);
        final String bucketPath = testPathPrefix + name;
        final String bucketObjectsDir = String.format("%s%s%s", bucketPath, SEPARATOR, "objects");
        Assert.assertTrue(mantaClient.createBucket(bucketPath));

        try (MantaBucketListingIterator itr = mantaClient.streamingBucketIterator(bucketObjectsDir, 10)) {
            Assert.assertFalse(itr.hasNext(), "There shouldn't be a next element");
            boolean failed = false;
            try {
                itr.next();
            } catch (NoSuchElementException e) {
                failed = true;
            }
            Assert.assertTrue(failed, "Iterator failed to throw NoSuchElementException");
        }

        mantaClient.deleteBucket(bucketPath);
        Assert.assertFalse(mantaClient.existsAndIsAccessible(bucketPath));
    }

    @Test
    public void createAndOverwriteExistingBucket() throws IOException {
        final String name = generateBucketName(testPathPrefix);
        final String bucketPath = testPathPrefix + name;
        Assert.assertTrue(mantaClient.createBucket(bucketPath));

        //Create another bucket with same name
        MantaAssert.assertResponseFailureStatusCode(409, BUCKET_EXISTS_ERROR,
                (MantaFunction<Object>) () -> mantaClient.createBucket(bucketPath));

        // Deleting created bucket after verification is complete
        mantaClient.deleteBucket(bucketPath);
        Assert.assertFalse(mantaClient.existsAndIsAccessible(bucketPath));
    }

    @Test
    public void checkNonExistentBucket() {
        final String name = generateBucketName(testPathPrefix);
        final String bucketPath = testPathPrefix + name;
        Assert.assertFalse(mantaClient.existsAndIsAccessible(bucketPath));
        Assert.assertThrows(MantaIOException.class, () -> mantaClient.deleteBucket(bucketPath));
    }

    @Test
    public void createBucketWithUnderscores() throws IOException {
        final String name = generateBucketName(testPathPrefix) + "___" + "754";
        final String bucketPath = testPathPrefix + name;
        verifyBucketNamingRestrictions(bucketPath);
    }

    @Test
    public void createBucketEndingWithNonAlphaNumeric() throws IOException {
        final String name = generateBucketName(testPathPrefix) + "-";
        final String bucketPath = testPathPrefix + name;
        verifyBucketNamingRestrictions(bucketPath);
    }

    @Test
    public void createBucketWithUpperCaseCharacters() throws IOException {
        final String name = generateBucketName(testPathPrefix).toUpperCase();
        final String bucketPath = testPathPrefix + name;
        verifyBucketNamingRestrictions(bucketPath);
    }

    @Test
    public void createBucketWithSubsequentPeriods() throws IOException {
        final String name = generateBucketName(testPathPrefix) + ".." + "118";
        final String bucketPath = testPathPrefix + name;
        verifyBucketNamingRestrictions(bucketPath);
    }

    @Test
    public void createBucketWithFormattedIPAddress() throws IOException {
        final String bucketPath = config.getMantaBucketsDirectory()
                + SEPARATOR +"192.0.2.0";
        verifyBucketNamingRestrictions(bucketPath);
    }

    @Test
    public void createBucketWithoutFormattedIPAddress() throws IOException {
        final String name = generateBucketName(testPathPrefix) + "172.25.1234.1";
        final String bucketPath = testPathPrefix + name;
        Assert.assertTrue(mantaClient.createBucket(bucketPath));
        mantaClient.deleteBucket(bucketPath);
        Assert.assertFalse(mantaClient.existsAndIsAccessible(bucketPath));
    }

    @Test
    public void createBucketWithErrorProneCharacters() throws IOException {
        final String name = generateBucketName(testPathPrefix) + ".-";
        final String bucketPath = testPathPrefix + name;
        verifyBucketNamingRestrictions(bucketPath);
    }

    @Test
    public void createBucketWithSmallAndLargeNames() throws IOException {
        final String name = generateBucketName(testPathPrefix) + "wehaveaverylargetbucketnamecomingoutexceedingmaxlimit64646466165";
        final String largeName = testPathPrefix + name;
        final String smallName = String.format(config.getMantaBucketsDirectory()+"%s%s", SEPARATOR, "ab");
        verifyBucketNamingRestrictions(largeName);
        verifyBucketNamingRestrictions(smallName);
    }

    @Test
    public void checkBucketObjectOperations() throws IOException {
        final String name = generateBucketName(testPathPrefix);
        final String bucketPath = testPathPrefix + name;
        final String objectPath = generateBucketObjectPath(bucketPath) + UUID.randomUUID().toString();

        Assert.assertTrue(mantaClient.createBucket(bucketPath));

        mantaClient.put(objectPath, TEST_DATA);
        Assert.assertTrue(mantaClient.existsAndIsAccessible(bucketPath));
        Assert.assertTrue(mantaClient.existsAndIsAccessible(objectPath));

        mantaClient.delete(objectPath);
        mantaClient.deleteBucket(bucketPath);
        Assert.assertFalse(mantaClient.existsAndIsAccessible(objectPath));
        Assert.assertFalse(mantaClient.existsAndIsAccessible(bucketPath));
    }

    @Test
    public final void testManyOperationsWithBuckets() throws IOException {
        final String name = generateBucketName(testPathPrefix);
        final String bucketPath = testPathPrefix + name;
        final String bucketObjectsDir = String.format("%s%s%s", bucketPath, SEPARATOR, "objects");
        Assert.assertTrue(mantaClient.createBucket(bucketPath));

        for (int i = 0; i < 100; i++) {
            final String objectPath = generateBucketObjectPath(bucketPath) + UUID.randomUUID().toString();
            mantaClient.put(objectPath, TEST_DATA);
            String actual = mantaClient.getAsString(objectPath);
            Assert.assertEquals(actual, TEST_DATA);
            mantaClient.delete(objectPath);

            MantaAssert.assertResponseFailureCode(404,
                    (MantaFunction<Object>) () -> mantaClient.get(objectPath));
        }
    }

    // TEST UTILITY METHODS

    private void verifyBucketNamingRestrictions(final String bucketPath) throws IOException {
        MantaAssert.assertResponseFailureStatusCode(422, INVALID_BUCKET_NAME_ERROR,
                (MantaFunction<Object>) () -> mantaClient.createBucket(bucketPath));
        Assert.assertFalse(mantaClient.existsAndIsAccessible(bucketPath));
    }

    private String generateBucketObjectPath(final String bucketPath) {
        return bucketPath + MantaUtils.formatPath(SEPARATOR + "objects"
                                + SEPARATOR);
    }
}
