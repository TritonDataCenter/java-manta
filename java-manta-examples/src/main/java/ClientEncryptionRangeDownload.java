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
        String mantaUserName = "wyatt";
        String privateKeyPath = "/Users/wyatt.lyonpreul/.ssh/id_rsa";
        String publicKeyId = "d4:19:cc:38:44:a8:5a:aa:76:1c:65:66:ba:08:1e:cb";

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
            headers.setByteRange(32L, 63L);

            // Print out every line from file streamed real-time from Manta
            InputStream is = client.getAsInputStream(mantaPath, headers);
            byte[] buffer = new byte[32];
            is.read(buffer);
            System.out.write(buffer);
        }
    }
}
