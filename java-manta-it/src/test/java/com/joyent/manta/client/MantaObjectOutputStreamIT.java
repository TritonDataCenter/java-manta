/*
 * Copyright (c) 2016-2019, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.client.helper.IntegrationTestHelper;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import com.joyent.manta.exception.MantaIOException;
import com.joyent.test.util.MantaAssert;
import com.joyent.test.util.MantaFunction;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.exception.ExceptionContext;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.UUID;

import static com.joyent.manta.exception.MantaErrorCode.RESOURCE_NOT_FOUND_ERROR;

/**
 * Tests for verifying the behavior of {@link MantaObjectOutputStream} with
 * {@link MantaClient}.
 */
@Test(groups = {"buckets"})
public class MantaObjectOutputStreamIT {
    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";

    private MantaClient mantaClient;

    private String testPathPrefix;

    @BeforeClass
    @Parameters({"encryptionCipher", "testType"})
    public void beforeClass(final @Optional String encryptionCipher,
                            final @Optional String testType) throws IOException {

        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new IntegrationTestConfigContext(encryptionCipher);
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

    public void canUploadSmallString() throws IOException {
        String path = testPathPrefix + "uploaded-" + UUID.randomUUID() + ".txt";
        MantaObjectOutputStream out = mantaClient.putAsOutputStream(path);

        try {
            out.write(TEST_DATA.getBytes(StandardCharsets.UTF_8));
        } finally {
            out.close();
        }

        MantaObject uploaded = out.getObjectResponse();

        Assert.assertEquals(uploaded.getContentLength().longValue(), TEST_DATA.length(),
                "Uploaded content length doesn't match");

        Assert.assertTrue(mantaClient.existsAndIsAccessible(path),
                "File wasn't uploaded: " + path);

        String actual = mantaClient.getAsString(path);
        Assert.assertEquals(actual, TEST_DATA,
                "Uploaded bytes don't match");

        mantaClient.delete(path);
        MantaAssert.assertResponseFailureStatusCode(404, RESOURCE_NOT_FOUND_ERROR,
                (MantaFunction<Object>) () -> mantaClient.head(path));
    }

    public void canUploadSmallStringWithErrorProneName() throws IOException {
        String path = testPathPrefix + "uploaded-" + UUID.randomUUID() + "- -~!@#$%^&*().txt";
        MantaObjectOutputStream out = mantaClient.putAsOutputStream(path);

        try {
            out.write(TEST_DATA.getBytes(StandardCharsets.UTF_8));
        } finally {
            out.close();
        }

        Assert.assertNotNull(out.getObjectResponse());

        Assert.assertTrue(mantaClient.existsAndIsAccessible(path),
                "File wasn't uploaded: " + path);

        mantaClient.delete(path);
        MantaAssert.assertResponseFailureStatusCode(404, RESOURCE_NOT_FOUND_ERROR,
                (MantaFunction<Object>) () -> mantaClient.head(path));
    }

    public void canUploadMuchLargerFile() throws IOException {
        String path = testPathPrefix + "uploaded-" + UUID.randomUUID() + ".txt";
        MantaObjectOutputStream out = mantaClient.putAsOutputStream(path);
        ByteArrayOutputStream bout = new ByteArrayOutputStream(6553600);

        MessageDigest md5Digest = DigestUtils.getMd5Digest();
        long totalBytes = 0;

        try {
            for (int i = 0; i < 100; i++) {
                int chunkSize = RandomUtils.nextInt(1, 131072);
                System.out.printf("[%03d] Writing to OutputStream with [%06d] sized chunk\n",
                        i+1, chunkSize);
                byte[] randomBytes = RandomUtils.nextBytes(chunkSize);
                md5Digest.update(randomBytes);
                totalBytes += randomBytes.length;
                out.write(randomBytes);
                bout.write(randomBytes);

                // periodically flush
                if (i % 25 == 0) {
                    System.out.println("  Flushing OutputStream");
                    out.flush();
                }
            }
        } finally {
            out.close();
            bout.close();
        }

        try (InputStream in = mantaClient.getAsInputStream(path)) {
            byte[] expected = bout.toByteArray();
            byte[] actual = IOUtils.readFully(in, (int)totalBytes);

            AssertJUnit.assertArrayEquals("Bytes written via OutputStream don't match read bytes",
                    expected, actual);
        }

        mantaClient.delete(path);
        MantaAssert.assertResponseFailureStatusCode(404, RESOURCE_NOT_FOUND_ERROR,
                (MantaFunction<Object>) () -> mantaClient.head(path));
    }

    public void canUploadMuchLargerFileWithPeriodicWaits() throws Exception {
        String path = testPathPrefix + "uploaded-" + UUID.randomUUID() + ".txt";
        MantaObjectOutputStream out = mantaClient.putAsOutputStream(path);
        ByteArrayOutputStream bout = new ByteArrayOutputStream(6553600);

        MessageDigest md5Digest = DigestUtils.getMd5Digest();
        long totalBytes = 0;
        int chunkSize = -1;

        Exception failureException = null;

        try {
            for (int i = 0; i < 100; i++) {
                chunkSize = RandomUtils.nextInt(1, 131072);
                final byte[] randomBytes = RandomUtils.nextBytes(chunkSize);
                System.out.printf("[%03d] Writing to OutputStream with [%06d] sized chunk\n",
                        i+1, chunkSize);
                md5Digest.update(randomBytes);
                totalBytes += randomBytes.length;
                out.write(randomBytes);
                bout.write(randomBytes);

                // periodically wait
                if (i % 3 == 0) {
                    final long waitTime = RandomUtils.nextLong(1L, 1000L);
                    System.out.printf("  Waiting for [%04d] ms\n", waitTime);
                    Thread.sleep(waitTime);
                }
            }
        } catch (MantaIOException e) {
            failureException = e;
        } finally {
            bout.close();

            try {
                out.close();
                // Test to see if the file was successfully created
                mantaClient.head(path);
            } catch (MantaIOException e) {
                failureException = e;
            }
        }

        if (failureException != null) {
            if (failureException instanceof ExceptionContext) {
                ExceptionContext e = (ExceptionContext)failureException;
                e.setContextValue("lastChunkSize", chunkSize);
                e.setContextValue("totalBytes", totalBytes);
            }
            throw failureException;
        }

        try (InputStream in = mantaClient.getAsInputStream(path)) {
            byte[] expected = bout.toByteArray();
            byte[] actual = IOUtils.readFully(in, (int)totalBytes);

            AssertJUnit.assertArrayEquals("Bytes written via "
                            + "OutputStream doesn't match read bytes",
                    expected, actual);
        }

        mantaClient.delete(path);
        MantaAssert.assertResponseFailureStatusCode(404, RESOURCE_NOT_FOUND_ERROR,
                (MantaFunction<Object>) () -> mantaClient.head(path));
    }
}
