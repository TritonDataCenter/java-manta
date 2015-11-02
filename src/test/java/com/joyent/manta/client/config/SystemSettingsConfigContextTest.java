package com.joyent.manta.client.config;

import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.EnvVarConfigContext;
import com.joyent.manta.config.SystemSettingsConfigContext;
import org.testng.annotations.Test;

import java.util.Properties;

import static com.joyent.manta.config.MapConfigContext.*;
import static org.testng.Assert.assertEquals;

public class SystemSettingsConfigContextTest {
    @Test(groups = { "config" })
    public void systemPropsOverwriteDefaults() {
        Properties properties = new Properties();
        properties.setProperty(MANTA_URL_KEY, "https://manta.triton.zone");
        properties.setProperty(MANTA_USER_KEY, "username");
        properties.setProperty(MANTA_KEY_ID_KEY, "00:00");
        properties.setProperty(MANTA_KEY_PATH_KEY, "/home/username/.ssh/foo_rsa");
        properties.setProperty(MANTA_TIMEOUT_KEY, "12");

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
    }

    @Test(groups = { "config" })
    public void environmentVarsTakePrecendenceOverSystemProps() {
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
}
