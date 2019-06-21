package com.joyent.manta.client.helper;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import org.apache.commons.lang3.BooleanUtils;
import org.testng.SkipException;

import java.io.IOException;
import java.util.UUID;

/**
 * A helper class that contains methods to assist in the Integration testing of
 * manta buckets functionality.
 *
 * @author <a href="https://github.com/1010sachin">Sachin Gupta</a>
 */

public class IntegrationTestHelper {

    private static String setupBucketsPath(final ConfigContext config, final String testName) {
        String bucketPathPrefix = String.format("%s%sbuckets%s%s", config.getMantaHomeDirectory(),
                MantaClient.SEPARATOR, MantaClient.SEPARATOR, testName + UUID.randomUUID());
        return bucketPathPrefix + MantaClient.SEPARATOR + "objects" + MantaClient.SEPARATOR;
    }

    private static boolean verifyBucketsSupport(final ConfigContext config,
                                               final MantaClient client) {
        try {
            String path = String.format("%s%sbuckets", config.getMantaHomeDirectory(),
                    MantaClient.SEPARATOR);
            client.options(path);
        } catch (IOException io) {
            return false;
        }
        return true;
    }

    public static void cleanupTestBucketOrDirectory(final MantaClient client,
                                          final String testPath) throws IOException {

        String bucketObjectsSubstring = String.format("%sobjects%s", MantaClient.SEPARATOR,
                MantaClient.SEPARATOR);
        if (testPath.contains(bucketObjectsSubstring)) {
            String bucketPath = testPath.substring(0, testPath.lastIndexOf(bucketObjectsSubstring));
            //Delete bucket here
        } else {
            IntegrationTestConfigContext.cleanupTestDirectory(client, testPath);
        }
    }

    public static String setupTestPath(final ConfigContext config,
                                       final MantaClient client,
                                       final String testName,
                                       final String testType) {
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
        } else {
            client.putDirectory(testPath, true);
        }

    }
}
