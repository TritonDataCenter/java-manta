package com.joyent.manta.client;

import com.joyent.manta.client.config.IntegrationTestConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.exception.MantaCryptoException;
import com.joyent.test.util.MantaAssert;
import com.joyent.test.util.MantaFunction;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.UUID;

import static com.joyent.manta.exception.MantaErrorCode.RESOURCE_NOT_FOUND_ERROR;

/**
 * Tests for verifying the correct functioning of making remote requests
 * against Manta directories.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
@Test(groups = { "directory" })
public class MantaClientDirectoriesIT {
    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";

    private MantaClient mantaClient;

    private String testPathPrefix;


    @BeforeClass
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
        testPathPrefix = String.format("/%s/stor/%s",
                config.getMantaHomeDirectory(), UUID.randomUUID());
    }


    @AfterClass
    public void afterClass() throws IOException, MantaCryptoException {
        if (mantaClient != null) {
            mantaClient.deleteRecursive(testPathPrefix);
            mantaClient.closeQuietly();
        }
    }


    @Test()
    public void canCreateDirectory() throws IOException {
        mantaClient.putDirectory(testPathPrefix);

        String dir = String.format("%s/%s", testPathPrefix, UUID.randomUUID());
        mantaClient.putDirectory(dir);

        MantaObject response = mantaClient.head(dir);
        Assert.assertEquals(dir, response.getPath());
    }


    @Test(dependsOnMethods = { "canCreateDirectory" })
    public void canDeleteDirectory() throws IOException {
        String dir = String.format("%s/%s", testPathPrefix, UUID.randomUUID());
        mantaClient.putDirectory(dir);

        MantaObject response = mantaClient.head(dir);
        Assert.assertEquals(dir, response.getPath());

        mantaClient.delete(dir);

        MantaAssert.assertResponseFailureStatusCode(404, RESOURCE_NOT_FOUND_ERROR,
                (MantaFunction<Object>) () -> mantaClient.get(dir));
    }

    @Test(dependsOnMethods = { "canCreateDirectory" })
    public void wontErrorWhenWeCreateOverAnExistingDirectory() throws IOException {
        String dir = String.format("%s/%s", testPathPrefix, UUID.randomUUID());
        mantaClient.putDirectory(dir);
        mantaClient.putDirectory(dir);
        mantaClient.putDirectory(dir);

        MantaObject response = mantaClient.head(dir);
        Assert.assertEquals(dir, response.getPath());
    }

    /**
     * This is somewhat surprising behavior, but this test documents Manta's
     * behavior. As a user of Manta, you will need to check to see if a
     * file exists before attempting to write a directory over the top of it.
     */
    @Test(dependsOnMethods = { "canCreateDirectory" })
    public void noErrorWhenWeOverwriteAnExistingFile() throws IOException {
        String dir = String.format("%s/%s", testPathPrefix, UUID.randomUUID());
        mantaClient.putDirectory(dir);

        String file = String.format("%s/%s", dir, UUID.randomUUID());
        mantaClient.put(file, TEST_DATA);
        mantaClient.putDirectory(file);
    }


    @Test(dependsOnMethods = { "canCreateDirectory" })
    public void directoryIsMarkedAsSuch() throws IOException {
        MantaObject dir = mantaClient.head(testPathPrefix);
        Assert.assertTrue(dir.isDirectory(),
                String.format("Directory should be marked as such [%s]", testPathPrefix));
    }


    @Test(dependsOnMethods = { "wontErrorWhenWeCreateOverAnExistingDirectory" })
    public void canRecursivelyCreateDirectory() throws IOException {
        String dir = String.format("%s/%s/%s/%s/%s/%s", testPathPrefix,
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID());

        mantaClient.putDirectory(dir, true);

        MantaObject response = mantaClient.head(dir);

        Assert.assertTrue(response.isDirectory(),
                String.format("Directory should be marked as such [%s]", testPathPrefix));
        Assert.assertEquals(dir, response.getPath());
    }
}
