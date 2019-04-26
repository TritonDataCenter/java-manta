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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

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

    public void authenticationModeCanBeSetToMandatory() {
        final String input = "Mandatory";
        final EncryptionAuthenticationMode expected = EncryptionAuthenticationMode.Mandatory;

        final Properties properties = new Properties();
        properties.setProperty(MANTA_ENCRYPTION_AUTHENTICATION_MODE_KEY, input);
        final SystemSettingsConfigContext instance = new SystemSettingsConfigContext(
                false, properties);

        final EncryptionAuthenticationMode actual = instance.getEncryptionAuthenticationMode();
        Assert.assertEquals(actual, expected, String.format(
                "[%s] set for [%s] didn't set the authentication mode correctly",
                input, MANTA_ENCRYPTION_AUTHENTICATION_MODE_KEY));
    }

    public void authenticationModeCanBeSetToOptional() {
        final String input = "Optional";
        final EncryptionAuthenticationMode expected = EncryptionAuthenticationMode.Optional;

        final Properties properties = new Properties();
        properties.setProperty(MANTA_ENCRYPTION_AUTHENTICATION_MODE_KEY, input);
        final SystemSettingsConfigContext instance = new SystemSettingsConfigContext(
                false, properties);

        final EncryptionAuthenticationMode actual = instance.getEncryptionAuthenticationMode();
        Assert.assertEquals(actual, expected, String.format(
                "[%s] set for [%s] didn't set the authentication mode correctly",
                input, MANTA_ENCRYPTION_AUTHENTICATION_MODE_KEY));
    }

    public void authenticationModeCanBeSetToVerificationDisabled() {
        final String input = "VerificationDisabled";
        final EncryptionAuthenticationMode expected = EncryptionAuthenticationMode.VerificationDisabled;

        final Properties properties = new Properties();
        properties.setProperty(MANTA_ENCRYPTION_AUTHENTICATION_MODE_KEY, input);
        final SystemSettingsConfigContext instance = new SystemSettingsConfigContext(
                false, properties);

        final EncryptionAuthenticationMode actual = instance.getEncryptionAuthenticationMode();
        Assert.assertEquals(actual, expected, String.format(
                "[%s] set for [%s] didn't set the authentication mode correctly",
                input, MANTA_ENCRYPTION_AUTHENTICATION_MODE_KEY));
    }

    // node-manta documents MANTA_TLS_INSECURE in terms of a 0/1, flag instead
    // of true/false.  Make sure what java-manta is doing is reasonable both in
    // terms of node-manta and java idioms
    public void testBooleanCoercion() {
        Properties propDefault = new Properties();
        ConfigContext configDefault = new SystemSettingsConfigContext(false, propDefault);
        assertFalse(configDefault.tlsInsecure());

        Properties propTrue = new Properties();
        propTrue.setProperty(MANTA_TLS_INSECURE_KEY, "true");
        ConfigContext configTrue = new SystemSettingsConfigContext(false, propTrue);
        assertTrue(configTrue.tlsInsecure());

        Properties propFalse = new Properties();
        propFalse.setProperty(MANTA_TLS_INSECURE_KEY, "false");
        ConfigContext configFalse = new SystemSettingsConfigContext(false, propFalse);
        assertFalse(configFalse.tlsInsecure());

        Properties propZero = new Properties();
        propZero.setProperty(MANTA_TLS_INSECURE_KEY, "0");
        ConfigContext configZero = new SystemSettingsConfigContext(false, propZero);
        assertFalse(configZero.tlsInsecure());

        Properties propOne = new Properties();
        propOne.setProperty(MANTA_TLS_INSECURE_KEY, "1");
        ConfigContext configOne = new SystemSettingsConfigContext(false, propOne);
        assertTrue(configOne.tlsInsecure());
    }

}
