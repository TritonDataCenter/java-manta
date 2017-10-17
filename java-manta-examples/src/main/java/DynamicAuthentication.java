/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

import com.joyent.manta.config.AuthAwareConfigContext;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.config.ChainedConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.SettableConfigContext;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class DynamicAuthentication {

    public static void main(String... args) throws IOException {

        // start with an unauthenticated user
        SettableConfigContext config = new ChainedConfigContext(
            new DefaultsConfigContext())
                .setMantaURL("https://us-east.manta.joyent.com")
                .setMantaUser("user/subuser")
                .setNoAuth(true);

        AuthAwareConfigContext authConfig = new AuthAwareConfigContext(config);

        try (MantaClient client = new MantaClient(authConfig)) {

            // read a public file
            String publicFile = "/user/public/foo";
            String privateFile = "/user/stor/bar";

            // Print out every line from file streamed real-time from Manta
            try (InputStream is = client.getAsInputStream(publicFile);
                 Scanner scanner = new Scanner(is, StandardCharsets.UTF_8.name())) {

                while (scanner.hasNextLine()) {
                    System.out.println(scanner.nextLine());
                }
            }

            // enable authentication
            config.setNoAuth(false);
            config.setMantaUser("user");
            config.setMantaKeyPath("src/test/java/data/id_rsa");
            config.setMantaKeyId("04:92:7b:23:bc:08:4f:d7:3b:5a:38:9e:4a:17:2e:df");
            authConfig.reload();

            // Load file into memory as a string directly from Manta
            String publicData = client.getAsString(publicFile);
            System.out.println(publicData);

            // Load a file in the private home folder
            String privateData = client.getAsString(privateFile);
            System.out.println(privateData);
        }
    }
}
