package com.joyent.manta.client;

import com.joyent.manta.client.config.IntegrationTestConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.exception.MantaCryptoException;
import org.testng.annotations.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class MantaMultipartIT {
    private MantaClient mantaClient;
    private MantaMultipart multipart;

    private String testPathPrefix;

    @BeforeClass()
    @Parameters({"manta.url", "manta.user", "manta.key_path", "manta.key_id", "manta.timeout", "manta.http_transport"})
    public void beforeClass(@Optional String mantaUrl,
                            @Optional String mantaUser,
                            @Optional String mantaKeyPath,
                            @Optional String mantaKeyId,
                            @Optional Integer mantaTimeout,
                            @Optional String mantaHttpTransport)
            throws IOException, MantaCryptoException {

        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new IntegrationTestConfigContext(
                mantaUrl, mantaUser, mantaKeyPath, mantaKeyId, mantaTimeout,
                mantaHttpTransport);

        this.mantaClient = new MantaClient(config);
        this.multipart = new MantaMultipart(this.mantaClient);
        testPathPrefix = String.format("%s/stor/%s/",
                config.getMantaHomeDirectory(), UUID.randomUUID());
        mantaClient.putDirectory(testPathPrefix);
    }


    @AfterClass
    public void afterClass() throws IOException, MantaCryptoException {
        if (mantaClient != null) {
            mantaClient.deleteRecursive(testPathPrefix);
            mantaClient.closeWithWarning();
        }
    }

    public void nonExistentFileHasNotStarted() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        assertFalse(multipart.isStarted(path));
    }

    public void canUploadSmallMultipartString() throws IOException {
        String[] parts = new String[] {
                "Hello ",
                "world ",
                "Joyent",
                "!"
        };

        StringBuilder combined = new StringBuilder();
        for (String p : parts) {
            combined.append(p);
        }

        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            int partNumber = i + 1;
            multipart.putPart(path, partNumber, part);
        }

        multipart.validateThereAreNoMissingParts(path);
    }
}
