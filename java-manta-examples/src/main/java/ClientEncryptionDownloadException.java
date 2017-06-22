/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.config.*;
import com.joyent.manta.exception.MantaClientEncryptionException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Scanner;

/*
* Usage: set the mantaUserName, privateKeyPath, and publicKeyId with your own values.
 */
public class ClientEncryptionDownloadException {

    public static void main(String... args) throws IOException {
        String mantaUserName = "USERNAME";
        String privateKeyPath = "PATH/.ssh/id_rsa";
        String publicKeyId = "04:92:7b:23:bc:08:4f:d7:3b:5a:38:9e:4a:17:2e:df";
        String mantaPath = "/" + mantaUserName + "/stor/foo";

        ConfigContext uploadConfig = new ChainedConfigContext(
                new DefaultsConfigContext(),
                new EnvVarConfigContext(),
                new MapConfigContext(System.getProperties()))
                .setMantaURL("https://us-east.manta.joyent.com")
                // If there is no subuser, then just use the account name
                .setMantaUser(mantaUserName)
                .setMantaKeyPath(privateKeyPath)
                .setMantaKeyId(publicKeyId)
                .setClientEncryptionEnabled(true)
                .setEncryptionAlgorithm("AES256/CTR/NoPadding")
                .setEncryptionAuthenticationMode(EncryptionAuthenticationMode.Optional)
                .setPermitUnencryptedDownloads(false)
                .setEncryptionKeyId("simple/example")
                .setEncryptionPrivateKeyBytes(Base64.getDecoder().decode("RkZGRkZGRkJEOTY3ODNDNkM5MUUyMjIyMTExMTIyMjI="));

        try (MantaClient uploadClient = new MantaClient(uploadConfig)) {
            MantaObjectResponse response = uploadClient.put(mantaPath, "This is my secret\nthat I want encrypted.");

            System.out.println("HTTP Response headers:");
            System.out.println(response.getHttpHeaders().toString());
        }

        // The private key is different than the one used for uploadClient
        ConfigContext downloadConfig = new ChainedConfigContext(
                new DefaultsConfigContext(),
                new EnvVarConfigContext(),
                new MapConfigContext(System.getProperties()))
                .setMantaURL("https://us-east.manta.joyent.com")
                // If there is no subuser, then just use the account name
                .setMantaUser(mantaUserName)
                .setMantaKeyPath(privateKeyPath)
                .setMantaKeyId(publicKeyId)
                .setClientEncryptionEnabled(true)
                .setEncryptionAlgorithm("AES256/CTR/NoPadding")
                .setEncryptionAuthenticationMode(EncryptionAuthenticationMode.Optional)
                .setPermitUnencryptedDownloads(false)
                .setEncryptionKeyId("simple/example")
                .setEncryptionPrivateKeyBytes(Base64.getDecoder().decode("RkZGRkZGRkJEOTY3ODNDNkM5MUUyMjIyMTExMTIyMjM="));

        try (MantaClient downloadClient = new MantaClient(downloadConfig)) {
            // Try to download and decrypt the file, handle the expected exception from an invalid key
            try (InputStream is = downloadClient.getAsInputStream(mantaPath);
                 Scanner scanner = new Scanner(is, StandardCharsets.UTF_8.name())) {

                while (scanner.hasNextLine()) {
                    System.out.println(scanner.nextLine());
                }
            } catch (MantaClientEncryptionException ex) {
                System.err.println(ex);
            }
        }
    }
}
