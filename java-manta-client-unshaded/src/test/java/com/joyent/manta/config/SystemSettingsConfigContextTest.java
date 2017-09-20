/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.config;

import com.joyent.manta.util.UnitTestConstants;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Properties;

import static com.joyent.manta.config.MapConfigContext.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

@Test
public class SystemSettingsConfigContextTest {
    @Test(groups = { "config" })
    public void systemPropsOverwriteDefaults() {
        Properties properties = new Properties();
        properties.setProperty(MANTA_URL_KEY, "https://manta.triton.zone");
        properties.setProperty(MANTA_USER_KEY, "username");
        properties.setProperty(MANTA_KEY_ID_KEY, "00:00");
        properties.setProperty(MANTA_KEY_PATH_KEY, "/home/username/.ssh/foo_rsa");
        properties.setProperty(MANTA_TIMEOUT_KEY, "12");
        properties.setProperty(MANTA_ENCRYPTION_KEY_ID_KEY, "my-unique-key");

        ConfigContext config = new SystemSettingsConfigContext(false, properties);

        assertEquals(config.getMantaURL(),
                properties.getProperty(MANTA_URL_KEY));
        assertEquals(config.getMantaUser(),
                properties.getProperty(MANTA_USER_KEY));
        assertEquals(config.getMantaKeyId(),
                properties.getProperty(MANTA_KEY_ID_KEY));
        assertEquals(config.getMantaKeyPath(),
                properties.getProperty(MANTA_KEY_PATH_KEY));
        assertEquals(String.valueOf(config.getTimeout()),
                properties.getProperty(MANTA_TIMEOUT_KEY));
        assertEquals(config.getEncryptionKeyId(),
                properties.getProperty(MANTA_ENCRYPTION_KEY_ID_KEY));
    }

    @Test(groups = { "config" })
    public void contextOverwritesDefaults() {
        final String expectedKeyPath = "/home/dude/.ssh/my_key";
        ConfigContext overwrite = mock(ConfigContext.class);
        when(overwrite.getMantaKeyPath()).thenReturn(expectedKeyPath);
        when(overwrite.getMantaURL()).thenReturn("https://manta.host.com");

        SystemSettingsConfigContext config = new SystemSettingsConfigContext(overwrite);

        Assert.assertEquals(config.getMantaKeyPath(), overwrite.getMantaKeyPath());
        Assert.assertEquals(config.getMantaURL(), overwrite.getMantaURL());
    }

    @Test(groups = { "config" })
    public void environmentVarsTakePrecedenceOverSystemProps() {
        Properties properties = new Properties();
        properties.setProperty(MANTA_URL_KEY, "https://manta.triton.zone");
        properties.setProperty(MANTA_USER_KEY, "username");
        properties.setProperty(MANTA_KEY_ID_KEY, "00:00");
        properties.setProperty(MANTA_KEY_PATH_KEY, "/home/username/.ssh/foo_rsa");
        properties.setProperty(MANTA_TIMEOUT_KEY, "12");

        EnvVarConfigContext envConfig = new EnvVarConfigContext();

        ConfigContext config = new SystemSettingsConfigContext(true, properties);

        if (envConfig.getMantaKeyId() != null) {
            assertEquals(config.getMantaKeyId(), envConfig.getMantaKeyId());
        }

        if (envConfig.getMantaURL() != null) {
            assertEquals(config.getMantaURL(), envConfig.getMantaURL());
        }

        if (envConfig.getMantaKeyPath() != null) {
            assertEquals(config.getMantaKeyPath(), envConfig.getMantaKeyPath());
        }

        if (envConfig.getMantaUser() != null) {
            assertEquals(config.getMantaUser(), envConfig.getMantaUser());
        }

        if (envConfig.getTimeout() != null) {
            assertEquals(config.getTimeout(), envConfig.getTimeout());
        }
    }

    // https://github.com/joyent/java-manta/issues/116
    public void verifyEmbeddedKeyOverwritesDefaultKeyPath() {
        final String user = "testuser";

        Properties properties = new Properties();
        properties.setProperty(MANTA_USER_KEY, user);
        properties.setProperty(MANTA_KEY_ID_KEY, UnitTestConstants.FINGERPRINT);
        properties.setProperty(MANTA_PRIVATE_KEY_CONTENT_KEY, UnitTestConstants.PRIVATE_KEY);

        // This shouldn't throw an error, but in defect #116 it does
        ConfigContext config = new SystemSettingsConfigContext(false, properties);
        assertEquals(config.getPrivateKeyContent(), UnitTestConstants.PRIVATE_KEY);
        assertEquals(config.getMantaKeyId(), UnitTestConstants.FINGERPRINT);
        assertEquals(config.getMantaUser(), user);
        assertNull(config.getMantaKeyPath());
    }
}
