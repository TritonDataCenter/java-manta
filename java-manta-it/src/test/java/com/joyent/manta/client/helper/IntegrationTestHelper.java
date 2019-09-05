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
import com.joyent.manta.util.MantaUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.SkipException;

import java.io.IOException;
import java.util.UUID;

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
    public static boolean verifyBucketsSupport(final ConfigContext config,
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
                    MantaIOException mioe = new MantaIOException("Cannot delete bucket containing objects", e);
                    mioe.setContextValue("bucket_path", testPath);
                    throw mioe;
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

    /**
     * Generates an allowed bucket name from the path provided.
     *
     * <p>This utility method is used to test only buckets related operations since
     * integration-tests have now been configured in a way to operate around a test-bucket named
     * java-integration-tests. This approach to creating buckets is viable and has potential usability
     * in other tests/integration-tests, where existing "testPathPrefix" generated by
     * {@link #setupBucketsPath(ConfigContext, String)} won't be used, for example in the test
     * {@link com.joyent.manta.client.MantaClientBucketsIT}.
     * </p>
     *
     * @param bucketPath URL or Unix-style file path
     * @return the generated bucket name
     */
    @SuppressWarnings("MagicNumber")
    public static String generateBucketName(final String bucketPath) {
        Validate.notEmpty(bucketPath, "Path to Bucket must not be null or empty");

        final String[] prefixes = MantaUtils.prefixPaths(bucketPath);
        if (!prefixes[1].contains("buckets")) {
            throw new IllegalArgumentException(
                    "Method was not used in the buckets directory");
        }

        final String name = MantaUtils.formatPath(MantaUtils.lastItemInPath(bucketPath) + UUID.randomUUID().toString());
        return name.toLowerCase().substring(0, RandomUtils.nextInt(5, 35)).
                replaceAll("[^a-zA-Z0-9]", StringUtils.EMPTY);
    }

    private static String parseBucketPath(final String testPath) {
        final int endIndex = testPath.lastIndexOf(bucketObjectsSubstring);
            return testPath.substring(0, endIndex);
    }
}
