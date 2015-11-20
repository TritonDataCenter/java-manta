package com.joyent.manta.client;

import com.joyent.manta.client.config.TestConfigContext;
import com.joyent.manta.config.ChainedConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.MapConfigContext;
import com.joyent.manta.exception.MantaCryptoException;
import com.joyent.test.util.MantaAssert;
import com.joyent.test.util.MantaFunction;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Properties;

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

    @BeforeClass
    @Parameters({"manta.url", "manta.user", "manta.key_path", "manta.key_id", "manta.timeout"})
    public void beforeClass(@Optional String mantaUrl,
                            @Optional String mantaUser,
                            @Optional String mantaKeyPath,
                            @Optional String mantaKeyId,
                            @Optional Integer mantaTimeout)
            throws IOException, MantaCryptoException {

        // Let TestNG configuration take precedence over environment variables
        config = new TestConfigContext(
                mantaUrl, mantaUser, mantaKeyPath, mantaKeyId, mantaTimeout);

        mantaClient = new MantaClient(config);
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

        MantaAssert.assertResponseFailureStatusCode(403,
                (MantaFunction<Object>) () -> badClient.head(path));
    }
}
