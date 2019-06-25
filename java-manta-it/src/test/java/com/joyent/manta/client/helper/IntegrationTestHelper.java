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
import org.apache.commons.lang3.BooleanUtils;
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

    private static String setupBucketsPath(final ConfigContext config, final String testName) {
        String bucketPathPrefix = String.format("%s%s%s", config.getMantaBucketsDirectory(),
                SEPARATOR, testName.toLowerCase() + UUID.randomUUID());

        return bucketPathPrefix + SEPARATOR + "objects" + SEPARATOR;
    }

    private static boolean verifyBucketsSupport(final ConfigContext config,
                                               final MantaClient client) throws IOException {
        try {
            String path = config.getMantaBucketsDirectory();
            client.options(path);
        } catch (MantaClientHttpResponseException me) {
            if (MantaErrorCode.RESOURCE_NOT_FOUND_ERROR.equals(me.getServerCode())) {
                return false;
            }
        }
        return true;
    }

    public static void cleanupTestBucketOrDirectory(final MantaClient client,
                                          final String testPath) throws IOException {
        String bucketObjectsSubstring = String.format("%sobjects%s", SEPARATOR,
                SEPARATOR);
        if (testPath.contains(bucketObjectsSubstring)) {
            String bucketPath = testPath.substring(0, testPath.lastIndexOf(bucketObjectsSubstring));
            //Delete bucket here
            client.deleteBucket(bucketPath);
        } else {
            IntegrationTestConfigContext.cleanupTestDirectory(client, testPath);
        }
    }

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

    public static void createTestBucketOrDirectory(final MantaClient client,
                                      final String testPath,
                                      final String testType) throws IOException {
        if ("buckets".equals(testType)) {
            //Create bucket here
            client.createBucket(testPath);
        } else {
            client.putDirectory(testPath, true);
        }
    }
}
