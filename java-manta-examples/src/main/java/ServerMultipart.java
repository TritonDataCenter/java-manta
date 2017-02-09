/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.multipart.*;
import com.joyent.manta.config.*;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.exception.ContextedRuntimeException;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

/*
* Usage: set the mantaUsername, privateKeyPath, publicKeyId, and multipartServer with your own values.
 */
public class ServerMultipart {
    private static String mantaUsername = "USERNAME";

    public static void main(String... args) {

        String privateKeyPath = "PATH/.ssh/id_rsa";
        String publicKeyId = "04:92:7b:23:bc:08:4f:d7:3b:5a:38:9e:4a:17:2e:df";
        String multipartServer = "https://MANTA_MULTIPART_SERVER";

        ConfigContext config = new ChainedConfigContext(
                new DefaultsConfigContext(),
                new EnvVarConfigContext(),
                new MapConfigContext(System.getProperties()))
                .setMantaURL(multipartServer)
                .setMantaUser(mantaUsername)
                .setMantaKeyPath(privateKeyPath)
                .setMantaKeyId(publicKeyId);


        try (MantaClient client = new MantaClient(config)) {
            ServerSideMultipartManager multipart = new ServerSideMultipartManager(client);
            multipartUpload(multipart);
        }
    }

    private static void multipartUpload(ServerSideMultipartManager multipart) {
        String uploadObject = "/" + mantaUsername + "/stor/multipart";


        // We catch network errors and handle them here
        try {
            ServerSideMultipartUpload upload = multipart.initiateUpload(uploadObject);
            MantaMultipartUploadPart part1 = multipart.uploadPart(upload, 1, RandomUtils.nextBytes(5242880));
            MantaMultipartUploadPart part2 = multipart.uploadPart(upload, 2, RandomUtils.nextBytes(1000000));

            // Complete the process by instructing Manta to assemble the final object from its parts
            MantaMultipartUploadTuple[] parts = new MantaMultipartUploadTuple[] { part1, part2 };
            Stream<MantaMultipartUploadTuple> partsStream = Arrays.stream(parts);
            multipart.complete(upload, partsStream);

            System.out.println(uploadObject + " is now assembled!");
        } catch (IOException e) {
            // This catch block is for general network failures
            // For example, ServerSideMultipartUpload.initiateUpload can throw an IOException

            ContextedRuntimeException exception = new ContextedRuntimeException(
                    "A network error occurred when doing a multipart upload to Manta.");
            exception.setContextValue("path", uploadObject);

            throw exception;
        }
    }
}
