package com.joyent.manta.client;

import com.joyent.manta.client.config.TestConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.exception.MantaCryptoException;
import org.testng.annotations.*;

import java.io.IOException;
import java.util.UUID;

/**
 * Tests for verifying the behavior of metadata with {@link MantaClient}.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaClientMetadataTest {
    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";

    private MantaClient mantaClient;

    private String testPathPrefix;

    @BeforeClass
    @Parameters({"manta.url", "manta.user", "manta.key_path", "manta.key_id", "manta.timeout"})
    public void beforeClass(@Optional String mantaUrl,
                            @Optional String mantaUser,
                            @Optional String mantaKeyPath,
                            @Optional String mantaKeyId,
                            @Optional Integer mantaTimeout)
            throws IOException {

        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new TestConfigContext(
                mantaUrl, mantaUser, mantaKeyPath, mantaKeyId, mantaTimeout);

        mantaClient = new MantaClient(config);
        testPathPrefix = String.format("/%s/stor/%s/", config.getMantaUser(), UUID.randomUUID());
        mantaClient.putDirectory(testPathPrefix, null);
    }


    @AfterClass
    public void cleanup() throws IOException {
        mantaClient.deleteRecursive(testPathPrefix);
    }


    @Test( groups = { "metadata" })
    public final void verifySimpleMetadataIsAddedOnPut() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.put("Yoda", "Master");
        headers.put("Droids", 1);
        headers.put("force", true);

        final MantaObject result = mantaClient.put(path, TEST_DATA, headers);
//        final MantaObject head = mantaClient.head(path);
//        final MantaObject get = mantaClient.get(path);

        MantaHttpHeaders headers2 = new MantaHttpHeaders();
        headers2.put("Yoda", "Master");
        headers2.put("Droids", 1);
        headers2.put("force", true);

        mantaClient.putMetadata(path, headers2);
        final MantaObject head2 = mantaClient.head(path);
        final MantaObject get2 = mantaClient.get(path);

        mantaClient.delete(path);
    }
}
