/*
 * Copyright (c) 2019, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.helper;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaErrorCode;
import com.joyent.manta.exception.MantaIOException;
import org.apache.commons.lang3.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.SkipException;

import java.io.IOException;

import static com.joyent.manta.client.MantaClient.SEPARATOR;

/**
 * A helper class that contains methods to assist in the Integration testing of
 * manta buckets functionality.
 *
 * @author <a href="https://github.com/1010sachin">Sachin Gupta</a>
 * @author <a href="https://github.com/nairashwin952013">Ashwin A Nair</a>
 */
public class IntegrationTestHelper {
    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestHelper.class);

    /**
     * Objects path appended to buckets directory (i.e /usr/buckets/$bucket_name/) storing all bucket objects.
     */
    private  static final String bucketObjectsSubstring = String.format("%sobjects%s"
            , SEPARATOR, SEPARATOR);

    /**
     * Base test-bucket where all SDK integration-tests will be run.
     */
    private static final String integrationTestBucket = "java-integration-tests";

    private static String setupBucketsPath(final ConfigContext config, final String testName) {
        final String bucketPathPrefix = String.format("%s%s%s", config.getMantaBucketsDirectory(),
                SEPARATOR, integrationTestBucket);
        return bucketPathPrefix + bucketObjectsSubstring + testName + "-";
    }

    /**
     * Verify Buckets support in Manta.
     * @see MantaClient#options(String)
     */
    private static boolean verifyBucketsSupport(final ConfigContext config,
                                               final MantaClient client) throws IOException {
        try {
            String path = config.getMantaBucketsDirectory();
            client.options(path);
        } catch (MantaClientHttpResponseException e) {
            if (MantaErrorCode.RESOURCE_NOT_FOUND_ERROR.equals(e.getServerCode())) {
                LOG.info("Buckets not supported in current Manta: {}", e.getStatusMessage());
            }
            return false;
        }
        return true;
    }

    /**
     * Deletes all test data for integration-tests.
     *
     * @param client Manta Client instance for the operations.
     * @param testPath The fully qualified test path for integration-tests.
     * @throws MantaClientHttpResponseException test-bucket being used for integration-tests is not empty.
     * @throws IOException If an IO exception has occurred.
     */
    public static void cleanupTestBucketOrDirectory(final MantaClient client,
                                          final String testPath) throws IOException {
        if (testPath.contains(bucketObjectsSubstring)) {
            final String bucket_path = parseBucketPath(testPath);

            try {
                client.deleteBucket(bucket_path);
            } catch (MantaClientHttpResponseException e) {
                if (e.getServerCode().equals(MantaErrorCode.BUCKET_NOT_EMPTY_ERROR)) {
                    e.setContextValue("Cleanup of non-empty test bucket attempted at:", testPath);
                    throw e;
                }
            }
        } else {
            IntegrationTestConfigContext.cleanupTestDirectory(client, testPath);
        }
    }

    /**
     * Formats base path for creation pertinent to integration-tests.
     *
     * @param config Configuration instance for the client.
     * @param client Manta Client instance for the operations.
     * @param testName Class name specific to each integration-test run.
     * @param testType value determining whether its a buckets or directory test.
     * @throws SkipException if Buckets tests are initiated without buckets support in Manta.
     * @throws IOException If an IO exception has occurred.
     */
    public static String setupTestPath(final ConfigContext config,
                                       final MantaClient client,
                                       final String testName,
                                       final String testType) throws IOException {
        if ("buckets".equals(testType)) {
            if (BooleanUtils.isTrue(verifyBucketsSupport(config, client))) {
                return setupBucketsPath(config, testName);
            } else {
                throw new SkipException("Bucket operations not supported by the server");
            }
        }

        return IntegrationTestConfigContext.generateBasePath(config, testName);
    }

    /**
     * Creates the base path leveraged by integration-tests for buckets/directories in Manta.
     *
     * @param client Manta Client instance for the operations.
     * @param testPath The fully qualified test path for integration-tests.
     * @param testType value determining whether its a buckets or directory test.
     * @return integration-test base path for both buckets and directories depending on testType.
     * @throws MantaClientHttpResponseException test-bucket being used for integration-tests already exist.
     * @throws IOException If an IO exception has occurred.
     */
    public static void createTestBucketOrDirectory(final MantaClient client,
                                      final String testPath,
                                      final String testType) throws IOException {
        if ("buckets".equals(testType)) {
            final String bucket_path = parseBucketPath(testPath);

            try {
                client.createBucket(bucket_path);
            } catch (MantaClientHttpResponseException e) {
                if (!e.getServerCode().equals(MantaErrorCode.BUCKET_EXISTS_ERROR)) {
                    e.setContextValue("Creating test bucket failed at :", testPath);
                    throw e;
                }
            } catch (IOException e) {
                throw new MantaIOException("Unable to create test bucket", e);
            }
        } else {
            client.putDirectory(testPath, true);
        }
    }

    private static String parseBucketPath(final String testPath) {
        final int endIndex = testPath.lastIndexOf(bucketObjectsSubstring);
            return testPath.substring(0, endIndex);
    }
}
