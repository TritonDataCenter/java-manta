/*
 * Copyright (c) 2019, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.SystemSettingsConfigContext;
import com.joyent.manta.http.MantaHttpHeaders;

import java.io.IOException;

import static com.joyent.manta.util.MantaUtils.formatPath;

/**
 * Creating a job using the `MantaClient` API is done by making a number of calls
 * against the API and passing the job id to each API call. Here is an example that
 * processes 4 input files, greps them for 'foo' and returns the unique values.
 */
public class BucketsWithMantaClient {
    public static void main(String... args) throws IOException {
        ConfigContext config = new SystemSettingsConfigContext();
        String bucketPath = "/user/buckets/foo";

        try (MantaClient client = new MantaClient(config)) {
            boolean created = client.createBucket(bucketPath);
            String testData = "Buckets, how you doing ?";
            String objectPath = bucketPath + MantaClient.SEPARATOR + "objects" + MantaClient.SEPARATOR;
            int count = 100;

            MantaMetadata metadata = new MantaMetadata();
            metadata.put("m-test-metadata", "any value");
            MantaHttpHeaders headers = new MantaHttpHeaders();
            headers.setContentType("text/plain");

            for (int i = 1; i <= count; i++) {
                String name = String.format("%05d", i);
                String path = formatPath(String.format("%s%s", objectPath, name));
                client.put(path, testData, headers, metadata);
            }
            for (int i = 1; i <= count; i++) {
                String name = String.format("%05d", i);
                String path = formatPath(String.format("%s%s", objectPath, name));
                client.delete(path);
            }

            if (created) {
                  client.deleteBucket(bucketPath);
            }
        }
    }
}
