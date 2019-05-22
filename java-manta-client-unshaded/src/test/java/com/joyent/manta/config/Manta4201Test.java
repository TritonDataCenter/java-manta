package com.joyent.manta.config;


import com.joyent.manta.client.MantaClient;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.testng.Assert.*;

/**
 * Tests for the bug described in JIRA MANTA-4201.
 *
 * Due to the loss of information about the provenance of the manta
 * key path field, it was possible for a *Config class to get into an
 * illegal state, where both mantaKeyPath and mantaKeyContent were
 * set. This was because a key path from a DefaultsConfigContext was
 * added to a *ChainConfig instance, but at one remove the
 * mantaKeyPathSetOnlyByDefaults field information was not passed
 * on. (See testManta4201Simple for the minimal example)
 */
@Test
public class Manta4201Test {
    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";

    @Test(groups = { "config" })
    public final void testManta4201Simple() {
        ConfigContext config = new ChainedConfigContext(new DefaultsConfigContext());
        ChainedConfigContext chained = new ChainedConfigContext(config);
        assertTrue(chained.isMantaKeyPathSetOnlyByDefaults(), "Path was set only by default");
    }

    @Test(groups = { "config" })
    public final void testManta4201() {
                EnvVarConfigContext authConfig = new EnvVarConfigContext();
                ConfigContext config = new ChainedConfigContext(
                new StandardConfigContext()
                        .setMantaKeyId(authConfig.getMantaKeyId())
                        .setMantaUser(authConfig.getMantaUser())
                        .setMantaURL(authConfig.getMantaURL())
                        .setRetries(1)
                        .setTcpSocketTimeout(5000)
                        .setTimeout(1500)
                        .setPrivateKeyContent("-----BEGIN RSA PRIVATE KEY-----\n" +
                                "Private Key Value \n" +
                                "-----END RSA PRIVATE KEY-----"),
                new DefaultsConfigContext()

        );

        DefaultsConfigContext defaults = new DefaultsConfigContext();

        ChainedConfigContext chained = new ChainedConfigContext(
                defaults, config);

        try {
            AuthAwareConfigContext aac = new AuthAwareConfigContext(chained);
        } catch (IllegalArgumentException e) {
            fail("Did not expect an IllegalArgumentException", e);
        } catch (Exception e) {
            assertTrue(true,
                    "Expected some error reading the (junk) private key content");
        }

    }

}
