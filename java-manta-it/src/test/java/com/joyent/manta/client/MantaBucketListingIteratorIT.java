/*
 * Copyright (c) 2019, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.client.helper.IntegrationTestHelper;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import org.apache.commons.lang3.Validate;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.joyent.manta.client.MantaClient.SEPARATOR;
import static com.joyent.manta.util.MantaUtils.formatPath;
import static org.apache.commons.lang3.StringUtils.substringAfterLast;

/**
 * Tests for verifying correct functioning of making remote requests i.e GET bucket(s)/object(s))
 * pertinent to list operations against Manta buckets.
 * <p>
 * Ensure that the testType parameter before running this class is configured to the value "buckets"
 * or else it will throw a {@link org.testng.SkipException}
 * </p>
 */
@Test(groups = { "buckets" })
public class MantaBucketListingIteratorIT {
    private static final String TEST_DATA = "I am Buckethead. We are Buckethead. We are Legion." +
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

    public final void testBucketListing() throws IOException {
        final String objectPath = testPathPrefix + UUID.randomUUID();
        final String objectPath1 = testPathPrefix + UUID.randomUUID();
        final String objectPath2 = testPathPrefix + UUID.randomUUID();

        mantaClient.put(objectPath, TEST_DATA);
        mantaClient.put(objectPath1, TEST_DATA);
        mantaClient.put(objectPath2, TEST_DATA);

        final String bucketObjectsDir = prefixObjectPath();
        final Stream<MantaObject> objs = mantaClient.listBucketObjects(bucketObjectsDir);
        final AtomicInteger count = new AtomicInteger(0);
        objs.forEach(obj -> {
            count.incrementAndGet();
            Assert.assertTrue(obj.getPath().startsWith(testPathPrefix));
        });

        Assert.assertEquals(3, count.get());

        mantaClient.delete(objectPath);
        mantaClient.delete(objectPath1);
        mantaClient.delete(objectPath2);
        Assert.assertFalse(mantaClient.existsAndIsAccessible(objectPath));
        Assert.assertFalse(mantaClient.existsAndIsAccessible(objectPath1));
        Assert.assertFalse(mantaClient.existsAndIsAccessible(objectPath2));
    }

    public void isPagingForBucketsCorrect() throws IOException {
        String basePath = String.format("%s%s", testPathPrefix, UUID.randomUUID());
        final String bucketObjectsDir = prefixObjectPath();
        final int MAX = 100;

        // Add files 1-100
        for (int i = 1; i <= MAX; i++) {
            String name = String.format("%05d", i);
            String path = String.format("%s%s", basePath, name);

            mantaClient.put(path, TEST_DATA);
        }

        try (MantaBucketListingIterator itr = mantaClient.streamingBucketIterator(bucketObjectsDir, 5)) {
            // Make sure we can get the first element
            Assert.assertTrue(itr.hasNext(), "We should have the first element");
            Map<String, Object> first = itr.next();
            final String baseName = substringAfterLast(basePath, SEPARATOR);
            Assert.assertEquals(first.get("name").toString(), baseName + "00001");

            // Scroll forward to the last element
            for (int i = 2; i < MAX; i++) {
                Assert.assertTrue(itr.hasNext(), "We should have the next element");
                Map<String, Object> next = itr.next();
                Assert.assertEquals(next.get("name").toString(), baseName + String.format("%05d", i));
            }

            // Make sure that we can get the last element
            Assert.assertTrue(itr.hasNext(), "We should have the last element");
            Map<String, Object> last = itr.next();
            Assert.assertEquals(last.get("name").toString(), baseName + String.format("%05d", MAX));

            // Make sure that we are at the end of the iteration
            Assert.assertFalse(itr.hasNext());

            boolean failed = false;

            try {
                itr.next();
            } catch (NoSuchElementException e) {
                failed = true;
            }

            Assert.assertTrue(failed, "Iterator failed to throw NoSuchElementException");
        }

        // Delete files 1-100
        for (int i = 1; i <= MAX; i++) {
            String name = String.format("%05d", i);
            String path = String.format("%s%s", basePath, name);

            mantaClient.delete(path);
        }
    }

    public void isPrefixPagingForBucketsCorrect() throws IOException {
        final String path1 = String.format("%s%s", testPathPrefix, UUID.randomUUID());
        final String path2 = String.format("%s%s", testPathPrefix, UUID.randomUUID());
        final String bucketObjectsDir = prefixObjectPath();
        final int pagingSize = 10;
        final int count1 = 20;
        final int count2 = 30;

        putMultipleObjects(path1, count1);
        putMultipleObjects(path2,count2);

        final String prefix1 = substringAfterLast(path1, SEPARATOR);
        final String prefix2 = substringAfterLast(path2, SEPARATOR);
        verifyPrefixPagingForBuckets(bucketObjectsDir, prefix1, pagingSize, count1);
        verifyPrefixPagingForBuckets(bucketObjectsDir, prefix2, pagingSize, count2);

        deleteMultipleObjects(path1, count1);
        deleteMultipleObjects(path2, count2);
    }

    public void testPrefixPaginationListingForBuckets() throws IOException {
        final String path1 = String.format("%s%s", testPathPrefix, "7d3d5d84-2124-443b-9257-680887befa07");
        final String path2 = String.format("%s%s", testPathPrefix, "594c434b-231a-4");
        final String bucketObjectsDir = prefixObjectPath();
        final int count1 = 20;
        final int count2 = 30;

        putMultipleObjects(path1, count1);
        putMultipleObjects(path2, count2);

        final String prefix1 = substringAfterLast(path1, SEPARATOR);
        final String prefix2 = substringAfterLast(path2, SEPARATOR);
        verifyListCountForBuckets(bucketObjectsDir, prefix1, count1);
        verifyListCountForBuckets(bucketObjectsDir, prefix2, count2);

        deleteMultipleObjects(path1, count1);
        deleteMultipleObjects(path2, count2);
    }

    public void isDelimiterPagingForBucketsCorrect() throws IOException {
        final String path = formatPath(String.format("%s%s", testPathPrefix, UUID.randomUUID()));
        final String bucketObjectsDir = prefixObjectPath();
        final char delimiter = testPathPrefix.charAt(testPathPrefix.length() - 1);
        final int count = 30;

        putMultipleObjects(path, count);
        verifyDelimiterPagingForBuckets(bucketObjectsDir,
                this.getClass().getSimpleName() + delimiter, delimiter);
        deleteMultipleObjects(path, count);
    }

    public void listBucketObjectsWithSmallPagingSize() throws IOException {
        final String path = formatPath(String.format("%s%s", testPathPrefix, UUID.randomUUID()));
        final String bucketObjectsDir = prefixObjectPath();
        final int pagingSize = 2;
        final int count = 60;

        putMultipleObjects(path, count);

        final String prefixName = substringAfterLast(path, SEPARATOR);
        verifyPrefixPagingForBuckets(bucketObjectsDir, prefixName, pagingSize, count);

        deleteMultipleObjects(path, count);
    }

    public void canListEmptyBucket() {
        final String bucketObjectsDir = prefixObjectPath();
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
    }

    public void isPagingForBucketsCorrectlyConcurrent() throws IOException {
        final String basePath = String.format("%s%s", testPathPrefix, UUID.randomUUID());
        final String bucketObjectsDir = prefixObjectPath();

        final int MAX = 100;
        final Map<String, Boolean> valuesFound = new ConcurrentHashMap<>(MAX);

        for (int i = 1; i <= MAX; i++) {
            final String name = String.format("%05d", i);
            final String path = String.format("%s%s", basePath, name);
            final String baseName = substringAfterLast(path, SEPARATOR);
            valuesFound.put(baseName, false);
            mantaClient.put(path, TEST_DATA);
        }

        try (MantaBucketListingIterator itr = mantaClient.streamingBucketIterator(bucketObjectsDir, 20)) {
            Runnable search = () -> {
                while (itr.hasNext()) {
                    try {
                        Map<String, Object> next = itr.next();
                        String name = next.get("name").toString();
                        valuesFound.replace(name, false, true);
                    } catch (NoSuchElementException e) {
                        /* we don't care about these exceptions because it is
                         * just another thread beating us to the end. */
                    }
                }
            };

            // Start 3 threads that iterate at the same time
            Thread thread1 = new Thread(search);
            Thread thread2 = new Thread(search);
            Thread thread3 = new Thread(search);

            thread1.start();
            thread2.start();
            thread3.start();

            // Wait in the current thread for them all to exit
            while (itr.hasNext()) {
                Thread.sleep(2000);

                if (!thread1.isAlive() && !thread2.isAlive() && !thread3.isAlive()) {
                    // one last check
                    if (itr.hasNext()) {
                        throw new AssertionError("We are in a dead-lock or a bad state");
                    }
                }
            }

            // Validate that all files were found
            valuesFound.forEach((key, value) -> Assert.assertTrue(value));
        } catch (InterruptedException e) {
            afterClass();
        }

        deleteMultipleObjects(basePath, MAX);
    }

    // TEST UTILITY METHODS

    private String prefixObjectPath() {
        final String objectPath = formatPath(testPathPrefix.substring(0, testPathPrefix.lastIndexOf(SEPARATOR)));

        Validate.notNull(objectPath, "Parsing object path failed");
        return objectPath;
    }

    private void putMultipleObjects(final String basePath, final int count) throws IOException{
        for (int i = 1; i <= count; i++) {
            String name = String.format("%05d", i);
            String path = formatPath(String.format("%s%s", basePath, name));
            mantaClient.put(path, TEST_DATA);
        }
    }

    private void deleteMultipleObjects(final String basePath, final int count) throws IOException{
        for (int i = 1; i <= count; i++) {
            String name = String.format("%05d", i);
            String path = formatPath(String.format("%s%s", basePath, name));
            mantaClient.delete(path);
        }
    }

    private void verifyListCountForBuckets(final String path,
                                           final String prefix,
                                           final int count) throws IOException {
        final Stream<MantaObject> objs = mantaClient.listBucketObjects(path, prefix);
        final AtomicInteger result = new AtomicInteger(0);

        objs.forEach(obj -> {
            result.incrementAndGet();
            Assert.assertTrue(obj.getPath().startsWith(testPathPrefix));
        });

        Assert.assertEquals(count, result.get());
    }

    private void verifyPrefixPagingForBuckets(final String path,
                                              final String prefix,
                                              final int pagingSize,
                                              final int count) {
        try (MantaBucketListingIterator itr = mantaClient.streamingBucketIterator(path, prefix, pagingSize)) {
            for (int i = 1; i <= count; i++) {
                Assert.assertTrue(itr.hasNext(), "We should have the next element");
                Map<String, Object> next = itr.next();
                Assert.assertEquals(next.get("name").toString(), prefix + String.format("%05d", i));
            }
        }
    }

    private void verifyDelimiterPagingForBuckets(final String path,
                                                 final String delimiterSubstr,
                                                 final Character delimiter) {
        try (MantaBucketListingIterator itr = mantaClient.streamingBucketIterator(path, delimiter, 5)) {
            for (int i = 1; i <= 30; i++) {
                Assert.assertTrue(itr.hasNext(), "We should have the next element");
                Map<String, Object> next = itr.next();
                Assert.assertEquals(next.get("name").toString(), delimiterSubstr);
            }
        }
    }
}
