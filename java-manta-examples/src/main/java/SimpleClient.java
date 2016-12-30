import com.joyent.manta.client.MantaClient;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.StandardConfigContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

public class SimpleClient {

    public static void main(String... args) throws IOException {
        ConfigContext config = new StandardConfigContext()
                .setMantaURL("https://us-east.manta.joyent.com")
                // If there is no subuser, then just use the account name
                .setMantaUser("user/subuser")
                .setMantaKeyPath("src/test/java/data/id_rsa")
                .setMantaKeyId("04:92:7b:23:bc:08:4f:d7:3b:5a:38:9e:4a:17:2e:df");

        try (MantaClient client = new MantaClient(config)) {
            String mantaFile = "/user/stor/foo";

            // Print out every line from file streamed real-time from Manta
            try (InputStream is = client.getAsInputStream(mantaFile);
                 Scanner scanner = new Scanner(is)) {

                while (scanner.hasNextLine()) {
                    System.out.println(scanner.nextLine());
                }
            }

            // Load file into memory as a string directly from Manta
            String data = client.getAsString(mantaFile);
            System.out.println(data);
        }
    }
}
