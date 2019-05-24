package com.joyent.manta.config;

import org.testng.annotations.Test;
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
 * on. (See testOneStepRemoveDefaultKeyPath for the minimal example)
 */
@Test
public class ChainedConfigKeyPathOriginTest {

    private static final String PRIVATE_KEY_CONTENT =
        "-----BEGIN RSA PRIVATE KEY-----\n" +
        "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\n" +
        "-----END RSA PRIVATE KEY-----";

    private static final String PRIVATE_KEY_ID = "MD5:66:2e:09:1b:6f:3d:1d:87:78:30:eb:24:9d:bf:b5:30";

    @Test(groups = { "config" })
    public final void testOneStepRemoveDefaultKeyPath() {
        ConfigContext config = new ChainedConfigContext(new DefaultsConfigContext());
        ChainedConfigContext chained = new ChainedConfigContext(config);
        assertTrue(chained.isMantaKeyPathSetOnlyByDefaults(), "Path was set only by default");
    }

    @Test(groups = { "config" })
    public final void testKeyContentNoKeyPathButByDefault() {
        EnvVarConfigContext authConfig = new EnvVarConfigContext();
        ConfigContext config = new ChainedConfigContext(
                                                        new StandardConfigContext()
                                                        .setMantaKeyId(authConfig.getMantaKeyId())
                                                        .setMantaUser(authConfig.getMantaUser())
                                                        .setMantaURL(authConfig.getMantaURL())
                                                        .setRetries(1)
                                                        .setTcpSocketTimeout(5000)
                                                        .setTimeout(1500)
                                                        .setMantaUser("my_user")
                                                        .setPrivateKeyContent(PRIVATE_KEY_CONTENT)
                                                        .setMantaKeyId(PRIVATE_KEY_ID),
                                                        new DefaultsConfigContext()
                                                        );

        DefaultsConfigContext defaults = new DefaultsConfigContext();

        ChainedConfigContext chained = new ChainedConfigContext(
                                                                defaults, config);

        try {
            AuthAwareConfigContext aac = new AuthAwareConfigContext(chained);
        } catch (IllegalArgumentException e) {
            fail("Did not expect an IllegalArgumentException", e);
        }
    }

}
