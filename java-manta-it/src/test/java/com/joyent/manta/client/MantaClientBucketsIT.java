/*
 * Copyright (c) 2019, Joyent, Inc. All rights reserved.
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
import com.joyent.test.util.RandomInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.joyent.manta.exception.MantaErrorCode.*;
import static com.joyent.manta.util.MantaUtils.generateBucketName;
import static com.joyent.manta.client.MantaClient.SEPARATOR;
import static org.testng.Assert.*;

/**
 * Tests for verifying buckets operations of {@link MantaClient}
 *
 * @author <a href="https://github.com/nairashwin952013">Ashwin A Nair</a>
 */
@Test(groups = { "buckets" })
public class MantaClientBucketsIT {

    private static final String TEST_FILENAME = "Master-Yoda.jpg";
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

    /*
    @AfterClass
    public void afterClass() throws IOException {
        IntegrationTestHelper.cleanupTestBucketOrDirectory(mantaClient, testPathPrefix);
    }
    */

    public void createAndDeleteBucket() throws IOException {
        final String name = generateBucketName(testPathPrefix);
        final String bucketPath = testPathPrefix + name;
        Assert.assertTrue(mantaClient.createBucket(bucketPath));
        mantaClient.deleteBucket(bucketPath);
        Assert.assertFalse(mantaClient.existsAndIsAccessible(bucketPath));
    }

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

    public void checkNonExistentBucket() {
        final String name = generateBucketName(testPathPrefix);
        final String bucketPath = testPathPrefix + name;
        Assert.assertFalse(mantaClient.existsAndIsAccessible(bucketPath));
        Assert.assertThrows(MantaIOException.class, () -> mantaClient.deleteBucket(bucketPath));
    }

    public void createBucketWithUnderscores() throws IOException {
        final String name = generateBucketName(testPathPrefix) + "___" + "754";
        final String bucketPath = testPathPrefix + name;
        verifyBucketNamingRestrictions(bucketPath);
    }

    public void createBucketEndingWithNonAlphaNumeric() throws IOException {
        final String name = generateBucketName(testPathPrefix) + "-";
        final String bucketPath = testPathPrefix + name;
        verifyBucketNamingRestrictions(bucketPath);
    }

    public void createBucketWithUpperCaseCharacters() throws IOException {
        final String name = generateBucketName(testPathPrefix).toUpperCase();
        final String bucketPath = testPathPrefix + name;
        verifyBucketNamingRestrictions(bucketPath);
    }

    public void createBucketWithSubsequentPeriods() throws IOException {
        final String name = generateBucketName(testPathPrefix) + ".." + "118";
        final String bucketPath = testPathPrefix + name;
        verifyBucketNamingRestrictions(bucketPath);
    }

    public void createBucketWithFormattedIPAddress() throws IOException {
        final String bucketPath = config.getMantaBucketsDirectory()
                + SEPARATOR +"192.0.2.0";
        verifyBucketNamingRestrictions(bucketPath);
    }

    public void createBucketWithoutFormattedIPAddress() throws IOException {
        final String name = generateBucketName(testPathPrefix) + "172.25.1234.1";
        final String bucketPath = testPathPrefix + name;
        Assert.assertTrue(mantaClient.createBucket(bucketPath));
        mantaClient.deleteBucket(bucketPath);
        Assert.assertFalse(mantaClient.existsAndIsAccessible(bucketPath));
    }

    public void createBucketWithErrorProneCharacters() throws IOException {
        final String name = generateBucketName(testPathPrefix) + ".-";
        final String bucketPath = testPathPrefix + name;
        verifyBucketNamingRestrictions(bucketPath);
    }

    public void createBucketWithSmallAndLargeNames() throws IOException {
        final String name = generateBucketName(testPathPrefix) + "wehaveaverylargetbucketnamecomingoutexceedingmaxlimit64646466165";
        final String largeName = testPathPrefix + name;
        final String smallName = String.format(config.getMantaBucketsDirectory()+"%s%s", SEPARATOR, "ab");
        verifyBucketNamingRestrictions(largeName);
        verifyBucketNamingRestrictions(smallName);
    }

    public void deleteRecursiveForBuckets() throws IOException {
        final String largeName = testPathPrefix + "mantaclientputit7d3d5d84-2124-443b-9257-680887befa07"
                +  "/objects/";
        mantaClient.deleteRecursive(largeName);
        mantaClient.deleteBucket(largeName);

    }

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
    public final void testListBucketObjects() throws IOException {
        final String name = generateBucketName(testPathPrefix);
        final String bucketPath = testPathPrefix + name;
        final String objectPath = generateBucketObjectPath(bucketPath);

        assertTrue(mantaClient.createBucket(bucketPath));

        mantaClient.put(objectPath + UUID.randomUUID().toString(), TEST_DATA);
        mantaClient.put(objectPath + UUID.randomUUID().toString(), TEST_DATA);
        mantaClient.put(objectPath + UUID.randomUUID().toString(), TEST_DATA);

        final Stream<MantaObject> objs = mantaClient.listBucketObjects(objectPath);

        final AtomicInteger count = new AtomicInteger(0);
        objs.forEach(obj -> {
            count.incrementAndGet();
            assertTrue(obj.getPath().startsWith(bucketPath));
        });

        assertEquals(3, count.get());
    }

    @Test
    public final void testPutWithByteArrayAndErrorProneCharacters() throws IOException {
        final String name = generateBucketName(testPathPrefix);
        final String bucketPath = testPathPrefix + name;
        final String objectPath = generateBucketObjectPath(bucketPath)
                + UUID.randomUUID().toString() + "- -!@#$%^&*().bin";

        Assert.assertTrue(mantaClient.createBucket(bucketPath));

        final byte[] content = TEST_DATA.getBytes(StandardCharsets.UTF_8);

        MantaObject response = mantaClient.put(objectPath, content);
        String contentType = response.getContentType();
        assertEquals(contentType, "application/octet-stream",
                "Content type wasn't set to expected default");

        final String actual = mantaClient.getAsString(objectPath, StandardCharsets.UTF_8);

        Assert.assertEquals(actual, TEST_DATA,
                "Uploaded byte array was malformed");
        mantaClient.delete(objectPath);
        mantaClient.deleteBucket(bucketPath);

        // the object should not exist
        Assert.assertFalse(mantaClient.existsAndIsAccessible(objectPath));
        Assert.assertFalse(mantaClient.existsAndIsAccessible(bucketPath));
    }

    @Test
    public final void testPutWithErrorProneCharacters() throws IOException {
        final String name = generateBucketName(testPathPrefix);
        final String bucketPath = testPathPrefix + name;
        final String objectPath = generateBucketObjectPath(bucketPath) + UUID.randomUUID().toString()
                + "- -`~!@#$%^&*().txt";

        assertTrue(mantaClient.createBucket(bucketPath));

        MantaObject response = mantaClient.put(objectPath, TEST_DATA, StandardCharsets.UTF_8);
        try (MantaObjectInputStream object = mantaClient.getAsInputStream(objectPath)) {
            String actual = IOUtils.toString(object, StandardCharsets.UTF_8);

            Assert.assertEquals(actual, TEST_DATA,
                    "Uploaded string didn't match expectation");
            Assert.assertEquals(response.getPath(), objectPath, "path not returned as written");
            Assert.assertEquals(object.getPath(), objectPath, "path not returned as written");

            mantaClient.delete(objectPath);
            mantaClient.deleteBucket(bucketPath);

            // the object should not exist
            Assert.assertFalse(mantaClient.existsAndIsAccessible(objectPath));
            Assert.assertFalse(mantaClient.existsAndIsAccessible(bucketPath));
        }
    }

    @Test
    public final void testPutWithFileInputStreamAndNoContentLength() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        File temp = Files.createTempFile("name", ".data").toFile();
        FileUtils.forceDeleteOnExit(temp);
        FileUtils.writeStringToFile(temp, TEST_DATA, StandardCharsets.UTF_8);

        // Test putting with an unknown content length
        try (FileInputStream in = new FileInputStream(temp)){
            mantaClient.put(path, in);
        }

        try (final MantaObjectInputStream gotObject = mantaClient.getAsInputStream(path)) {
            Assert.assertNotNull(gotObject);
            Assert.assertNotNull(gotObject.getContentType());
            Assert.assertNotNull(gotObject.getContentLength());
            Assert.assertNotNull(gotObject.getEtag());
            Assert.assertNotNull(gotObject.getMtime());
            Assert.assertNotNull(gotObject.getPath());

            final String data = IOUtils.toString(gotObject, Charset.defaultCharset());
            Assert.assertEquals(data, TEST_DATA);
        }

        mantaClient.delete(path);

        MantaAssert.assertResponseFailureStatusCode(404, RESOURCE_NOT_FOUND_ERROR,
                (MantaFunction<Object>) () -> mantaClient.get(testPathPrefix + name));
    }

    @Test
    public final void testPutWithPlainTextFileWithErrorProneName() throws IOException {
        final String name = UUID.randomUUID().toString() + "- -~!@#$%^&*().txt";
        final String path = testPathPrefix + name;
        File temp = File.createTempFile("upload", ".txt");
        FileUtils.forceDeleteOnExit(temp);

        Files.write(temp.toPath(), TEST_DATA.getBytes(StandardCharsets.UTF_8));
        MantaObject response = mantaClient.put(path, temp);
        String contentType = response.getContentType();
        Assert.assertEquals(contentType, "text/plain",
                "Content type wasn't detected correctly");

        String actual = mantaClient.getAsString(path);
        Assert.assertEquals(actual, TEST_DATA,
                "Uploaded file didn't match expectation");
        Assert.assertEquals(response.getPath(), path, "path returned as written");
    }

    @Test
    public final void testPutWithStreamAndErrorProneName() throws IOException {
        final String name = UUID.randomUUID().toString() + "- -!@#$%^&*().txt";
        final String path = testPathPrefix + name;
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Assert.assertNotNull(classLoader.getResource(TEST_FILENAME));

        final int length = mantaClient.getContext().getUploadBufferSize() + 1024;

        try (InputStream testDataInputStream = new RandomInputStream(length)) {
            Assert.assertFalse(testDataInputStream.markSupported());
            mantaClient.put(path, testDataInputStream);
        }
        try (MantaObjectInputStream object = mantaClient.getAsInputStream(path)) {
            Assert.assertEquals(object.getPath(), path, "path not returned as written");
            byte[] actualBytes = IOUtils.readFully(object, length);
            Assert.assertTrue(actualBytes.length > 0);
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
