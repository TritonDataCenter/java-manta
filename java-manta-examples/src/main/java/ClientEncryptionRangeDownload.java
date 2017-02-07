import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.config.*;
import com.joyent.manta.http.MantaHttpHeaders;

import java.util.Base64;
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;


/*
* Usage: set the mantaUserName, privateKeyPath, and publicKeyId with your own values.
 */
public class ClientEncryptionRangeDownload {

    public static void main(String... args) throws IOException {
        String mantaUserName = "USERNAME";
        String privateKeyPath = "PATH/.ssh/id_rsa";
        String publicKeyId = "04:92:7b:23:bc:08:4f:d7:3b:5a:38:9e:4a:17:2e:df";

        ConfigContext config = new ChainedConfigContext(
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

        try (MantaClient client = new MantaClient(config)) {
            String mantaPath = "/" + mantaUserName + "/stor/foo";

            // Divided into 16 byte block chunks for testing
            final String plaintext = "This is my secre" +
                                     "t that I want en" +
                                     "crypted. And her" +
                                     "e is more text.";

            MantaObjectResponse response = client.put(mantaPath, plaintext);

            System.out.println("HTTP Response headers:");
            System.out.println(response.getHttpHeaders().toString());

            MantaHttpHeaders headers = new MantaHttpHeaders();
            headers.setByteRange(0L, 15L);

            try (InputStream is = client.getAsInputStream(mantaPath, headers);
                 Scanner scanner = new Scanner(is)) {

                while (scanner.hasNextLine()) {
                    System.out.println(scanner.nextLine());
                }
            }
        }
    }
}
