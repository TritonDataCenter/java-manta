/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.multipart.JobsMultipartManager;
import com.joyent.manta.client.multipart.JobsMultipartUpload;
import com.joyent.manta.client.multipart.MantaMultipartUploadPart;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.SystemSettingsConfigContext;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.commons.lang3.exception.ContextedRuntimeException;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class JobsBasedMultipart {
    public static void main(String... args) {
        ConfigContext config = new SystemSettingsConfigContext();

        try (MantaClient client = new MantaClient(config)) {
            multipartUpload(client);
        }
    }

    private static void multipartUpload(MantaClient mantaClient) {
        // instantiated with a reference to the class the actually connects to Manta
        JobsMultipartManager multipart = new JobsMultipartManager(mantaClient);

        String uploadObject = "/username/stor/test/file";

    /* I'm using File objects below, but I could be using byte[] arrays,
     * Strings, or InputStreams as well. */
        File part1file = new File("part-1.data");
        File part2file = new File("part-2.data");
        File part3file = new File("part-3.data");

        // We can set any metadata for the final object
        MantaMetadata metadata = new MantaMetadata();
        metadata.put("m-test-metadata", "any value");

        // We can set any header for the final object
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setContentType("text/plain");

        // We catch network errors and handle them here
        try {
            // We get a response object
            JobsMultipartUpload upload = multipart.initiateUpload(uploadObject);

            // It contains a UUID transaction id
            UUID id = upload.getId();
            // It also contains the path of the final object
            String uploadPath = upload.getPath();

            // Everywhere below that we specified "upload" we could also just
            // use the upload transaction id

            List<MantaMultipartUploadPart> parts = new ArrayList<>();

            // We can add the parts in any order
            MantaMultipartUploadPart part2 = multipart.uploadPart(upload, 2, part2file);
            // Each put of a part is a synchronous operation
            MantaMultipartUploadPart part1 = multipart.uploadPart(upload, 1, part1file);
            // Although in a later version we could make an async option
            MantaMultipartUploadPart part3 = multipart.uploadPart(upload, 3, part3file);

            parts.add(part1);
            parts.add(part3);
            parts.add(part2);

            // If we want to give up now, we could always abort
            // multipart.abort(upload);

            // We've uploaded all of the parts, now lets join them
            multipart.complete(upload, parts.stream());

            // If we want to pause execution until it is committed
            int timesToPoll = 10;
            multipart.waitForCompletion(upload, Duration.ofSeconds(5), timesToPoll,
                    uuid -> {
                        throw new RuntimeException("Multipart completion timed out");
                    });

        } catch (MantaClientHttpResponseException e) {
            // This catch block is for when we actually have a response code from Manta

            // We can handle specific HTTP responses here
            if (e.getStatusCode() == 503) {
                System.out.println("Manta is unavailable. Please try again");
                return;
            }

            // We could rethrow as a more detailed exception as below
            throw new RuntimeException(e);
        } catch (IOException e) {
            // This catch block is for general network failures
            // Note: MantaClientHttpResponseException inherits from IOException
            // so if it is not explicitly caught, it would go to this block

            ContextedRuntimeException exception = new ContextedRuntimeException(
                    "A network error occurred when doing a multipart upload to" +
                            "Manta. See context for details.");
            // We should all of the diagnostic context that we need
            exception.setContextValue("parts", "[part-1.data, part-2.data, part-3.data]");

            // We rethrow the exception with additional detail
            throw exception;
        }
    }
}
