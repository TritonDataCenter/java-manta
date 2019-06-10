package com.joyent.manta.client.helper;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.config.ConfigContext;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A helper class that contains methods to assist in the Integration testing of
 * manta buckets functionality.
 *
 * @author <a href="https://github.com/1010sachin">Sachin Gupta</a>
 */

public class IntegrationTestHelper {

    public static String setupBucketsPath(final ConfigContext config, final String testName) {
        String testPathPrefix = String.format("%s/buckets/%s", config.getMantaHomeDirectory(),
                testName + UUID.randomUUID());
        return testPathPrefix + MantaClient.SEPARATOR;
    }

    public static boolean verifyBucketsSupport(final ConfigContext config,
                                               final MantaClient client) {
        AtomicBoolean isBucketsSupported = new AtomicBoolean();
        try {
            String path = String.format("%s/buckets", config.getMantaHomeDirectory());
            client.get(path);
            isBucketsSupported.set(true);
        } catch (IOException io) {
            isBucketsSupported.set(false);
        }

        return isBucketsSupported.get();
    }

    public static String setupBucketObjectBasePath(final String bucketTestPathPrefix) {
        return bucketTestPathPrefix + "objects" + MantaClient.SEPARATOR;
    }

    public static String setupBucketObjectBasePathWithoutSeparator(final String bucketTestPathPrefix) {
        return bucketTestPathPrefix + "objects";
    }

    public static void cleanupTestBucket(final MantaClient client, final String bucketPath) {
        //Delete the bucket here
    }
}
