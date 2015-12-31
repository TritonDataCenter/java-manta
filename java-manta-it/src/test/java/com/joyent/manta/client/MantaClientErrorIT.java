package com.joyent.manta.client;

import com.joyent.manta.client.config.IntegrationTestConfigContext;
import com.joyent.manta.config.ChainedConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.MapConfigContext;
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
import java.util.Properties;
import java.util.UUID;

import static com.joyent.manta.exception.MantaErrorCode.ACCOUNT_DOES_NOT_EXIST_ERROR;
import static com.joyent.manta.exception.MantaErrorCode.NO_CODE_ERROR;
import static com.joyent.manta.exception.MantaErrorCode.RESOURCE_NOT_FOUND_ERROR;

/**
 * Tests for verifying the correct behavior of error handling from Manta API
 * failures.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
@Test(groups = { "error" })
public class MantaClientErrorIT {
    private MantaClient mantaClient;

    private ConfigContext config;

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
        config = new IntegrationTestConfigContext(
                mantaUrl, mantaUser, mantaKeyPath, mantaKeyId, mantaTimeout,
                mantaHttpTransport);

        mantaClient = new MantaClient(config);
        testPathPrefix = String.format("/%s/stor/%s",
                config.getMantaHomeDirectory(), UUID.randomUUID());
        mantaClient.putDirectory(testPathPrefix);
    }

    @AfterClass
    public void afterClass() throws IOException, MantaCryptoException {
        if (mantaClient != null) {
            mantaClient.deleteRecursive(testPathPrefix);
            mantaClient.closeQuietly();
        }
    }

    @Test
    public void incorrectLogin() throws IOException {
        Properties properties = new Properties();
        properties.setProperty(MapConfigContext.MANTA_USER_KEY, "baduser");

        ConfigContext badConfig = new ChainedConfigContext(
                config,
                new MapConfigContext(properties)
        );

        Assert.assertEquals(badConfig.getMantaUser(), "baduser",
                "Unable to set incorrect account");

        final String path = String.format("%s/stor", config.getMantaHomeDirectory());
        final MantaClient badClient = new MantaClient(badConfig);

        MantaAssert.assertResponseFailureStatusCode(403, ACCOUNT_DOES_NOT_EXIST_ERROR,
                (MantaFunction<Object>) () -> badClient.get(path));
    }

    @Test
    public void badHomeDirectory() throws IOException {
        String path = "/badpath";

        MantaAssert.assertResponseFailureStatusCode(403, ACCOUNT_DOES_NOT_EXIST_ERROR,
                (MantaFunction<Object>) () -> mantaClient.get(path));
    }

    @Test
    public void fileNotFoundWithNoContent() throws IOException {
        String path = String.format("%s/%s", testPathPrefix, UUID.randomUUID());

        MantaAssert.assertResponseFailureStatusCode(404, NO_CODE_ERROR,
                (MantaFunction<Object>) () -> mantaClient.head(path));
    }


    @Test
    public void fileNotFoundWithContent() throws IOException {
        String path = String.format("%s/%s", testPathPrefix, UUID.randomUUID());

        MantaAssert.assertResponseFailureStatusCode(404, RESOURCE_NOT_FOUND_ERROR,
                (MantaFunction<Object>) () -> mantaClient.get(path));
    }
}
