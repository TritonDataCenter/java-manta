/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.config.*;

import java.util.Base64;
import java.io.IOException;

/*
* Usage: set the mantaUserName, privateKeyPath, and publicKeyId with your own values.
 */
public class ClientEncryptionMetadata {

    public static void main(String... args) throws IOException {
        String mantaUserName = "USERNAME";
        String privateKeyPath = "PATH/.ssh/id_rsa";
        String publicKeyId = "04:92:7b:23:bc:08:4f:d7:3b:5a:38:9e:4a:17:2e:df";

        ConfigContext config = new ChainedConfigContext(
                new DefaultsConfigContext(),
                new EnvVarConfigContext(),
                new MapConfigContext(System.getProperties()))
                .setMantaURL("https://us-east.manta.joyent.com")
                .setMantaUser(mantaUserName)
                .setMantaKeyPath(privateKeyPath)
                .setMantaKeyId(publicKeyId)
                .setClientEncryptionEnabled(true)
                .setEncryptionAlgorithm("AES256/CTR/NoPadding")
                .setEncryptionAuthenticationMode(EncryptionAuthenticationMode.Mandatory)
                .setPermitUnencryptedDownloads(false)
                .setEncryptionKeyId("simple/example")
                .setEncryptionPrivateKeyBytes(Base64.getDecoder().decode("RkZGRkZGRkJEOTY3ODNDNkM5MUUyMjIyMTExMTIyMjI="));

        try (MantaClient client = new MantaClient(config)) {
            String mantaPath = "/" + mantaUserName + "/stor/meta";

            MantaMetadata metadata = new MantaMetadata();
            metadata.put("e-secretkey", "My Secret Value");
            MantaObjectResponse putRes = client.put(mantaPath, "This is my secret message", metadata);

            System.out.println("HTTP PUT Response headers:");
            System.out.println(putRes.getHttpHeaders().toString());


            MantaObjectResponse getRes = client.get(mantaPath);
            MantaMetadata resMetadata = getRes.getMetadata();

            System.out.println("Stored Metadata:");
            System.out.println(resMetadata.get("e-secretkey"));
        }
    }
}
