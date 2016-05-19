package com.joyent.manta.client;

import com.joyent.manta.client.config.IntegrationTestConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.exception.MantaCryptoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.IOException;
import java.util.UUID;

@Test
public class MantaObjectOutputStreamIT {
    private static final Logger LOG = LoggerFactory.getLogger(MantaClientJobIT.class);
    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";

    private MantaClient mantaClient;

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

        mantaClient = new MantaClient(config);
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

    public void canUpload() throws IOException {
        String path = testPathPrefix + "uploaded-" + UUID.randomUUID() + ".txt";
        try (MantaObjectOutputStream out = mantaClient.putAsOutputStream(path)) {
            out.write(TEST_DATA.getBytes());
        }

        Assert.assertTrue(mantaClient.existsAndIsAccessible(path),
                "File wasn't uploaded: " + path);

        String actual = mantaClient.getAsString(path);
        Assert.assertEquals(actual, TEST_DATA,
                "Uploaded bytes don't match");
    }
}
