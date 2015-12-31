package com.joyent.manta.client.config;

import com.joyent.manta.config.ConfigContext;
import org.testng.annotations.Test;

import java.util.Properties;

import static com.joyent.manta.config.MapConfigContext.*;
import static org.testng.Assert.assertEquals;

public class TestConfigContextTest {

    @Test(groups = { "config" })
    public void testNGparamsOverwriteDefaults() {
        ConfigContext testNgContext = TestConfigContext.buildTestContext(
                "https://us-west.manta.joyent.com",
                "bob",
                "/home/bob/.ssh/bar_rsa",
                "11:11",
                12,
                5,
                "NetHttpTransport");

        Properties properties = new Properties();

        ConfigContext config = new TestConfigContext(testNgContext, properties,
                false);

        assertEquals(config.getMantaURL(), "https://us-west.manta.joyent.com");
        assertEquals(config.getMantaUser(), "bob");
        assertEquals(config.getMantaKeyId(), "11:11");
        assertEquals(config.getMantaKeyPath(), "/home/bob/.ssh/bar_rsa");
        assertEquals((int)config.getTimeout(), 12);
        assertEquals((int)config.getRetries(), 5);
        assertEquals(config.getHttpTransport(), "NetHttpTransport");
    }

    @Test(groups = { "config" })
    public void systemPropsTakePrecedenceOverTestNGparams() {
        ConfigContext testNgContext = TestConfigContext.buildTestContext(
                "https://us-west.manta.joyent.com",
                "bob",
                "/home/bob/.ssh/bar_rsa",
                "11:11",
                12,
                5,
                "NetHttpTransport");

        Properties properties = new Properties();
        properties.setProperty(MANTA_URL_KEY, "https://manta.triton.zone");
        properties.setProperty(MANTA_USER_KEY, "username");
        properties.setProperty(MANTA_KEY_ID_KEY, "00:00");
        properties.setProperty(MANTA_KEY_PATH_KEY, "/home/username/.ssh/foo_rsa");
        properties.setProperty(MANTA_TIMEOUT_KEY, "12");
        properties.setProperty(MANTA_RETRIES_KEY, "24");
        properties.setProperty(MANTA_HTTP_TRANSPORT_KEY, "MockHttpTransport");

        ConfigContext config = new TestConfigContext(testNgContext, properties,
                false);

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
        assertEquals(String.valueOf(config.getRetries()),
                properties.getProperty(MANTA_RETRIES_KEY));
        assertEquals(String.valueOf(config.getHttpTransport()),
                properties.getProperty(MANTA_HTTP_TRANSPORT_KEY));
    }
}
