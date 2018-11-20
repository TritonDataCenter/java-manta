/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.config;

import com.joyent.manta.client.crypto.AesGcmCipherDetails;
import com.joyent.manta.exception.ConfigurationException;
import com.joyent.manta.util.UnitTestConstants;
import org.apache.commons.io.FileUtils;
import org.apache.http.protocol.HttpRequestExecutor;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Test
public class ConfigContextTest {
    private final byte[] keyBytes;

    {
        keyBytes = "FFFFFFFBD96783C6C91E2222".getBytes(StandardCharsets.US_ASCII);
    }

    @Test(expectedExceptions = ConfigurationException.class)
    public void validationWillFailWithNothingSet() {
        StandardConfigContext config = new StandardConfigContext();
        ConfigContext.validate(config);
    }

    public void canValidateContextWithClientEncryptionDisabled() throws IOException {
        File mantaAuthPrivateKey = File.createTempFile("manta-key", "");
        FileUtils.forceDeleteOnExit(mantaAuthPrivateKey);
        FileUtils.write(mantaAuthPrivateKey, UnitTestConstants.PRIVATE_KEY, StandardCharsets.US_ASCII);

        StandardConfigContext config = new StandardConfigContext();
        config.setMantaURL(DefaultsConfigContext.DEFAULT_MANTA_URL);
        config.setMantaUser("username");
        config.setMantaKeyId(UnitTestConstants.FINGERPRINT);
        config.setMantaKeyPath(mantaAuthPrivateKey.getAbsolutePath());
        config.setClientEncryptionEnabled(false);

        ConfigContext.validate(config);
    }

    public void canValidateContextWithKeyPaths() throws IOException {
        File mantaAuthPrivateKey = File.createTempFile("manta-key", "");
        FileUtils.forceDeleteOnExit(mantaAuthPrivateKey);
        FileUtils.write(mantaAuthPrivateKey, UnitTestConstants.PRIVATE_KEY, StandardCharsets.US_ASCII);

        File encryptionPrivateKey = File.createTempFile("encryption-key", "");
        FileUtils.forceDeleteOnExit(encryptionPrivateKey);
        FileUtils.writeByteArrayToFile(encryptionPrivateKey, keyBytes);

        StandardConfigContext config = new StandardConfigContext();
        config.setMantaURL(DefaultsConfigContext.DEFAULT_MANTA_URL);
        config.setMantaUser("username");
        config.setMantaKeyId(UnitTestConstants.FINGERPRINT);
        config.setMantaKeyPath(mantaAuthPrivateKey.getAbsolutePath());
        config.setClientEncryptionEnabled(true);
        config.setEncryptionKeyId("test-key-1");
        config.setEncryptionAuthenticationMode(EncryptionAuthenticationMode.DEFAULT_MODE);
        config.setPermitUnencryptedDownloads(false);
        config.setEncryptionPrivateKeyPath(encryptionPrivateKey.getAbsolutePath());
        config.setEncryptionAlgorithm(AesGcmCipherDetails.INSTANCE_128_BIT.getCipherId());

        ConfigContext.validate(config);
    }

    public void canValidateContextWithKeyValues() throws IOException {
        StandardConfigContext config = new StandardConfigContext();
        config.setMantaURL(DefaultsConfigContext.DEFAULT_MANTA_URL);
        config.setMantaUser("username");
        config.setMantaKeyId("ac:95:92:ff:88:f7:3d:cd:ba:23:7b:54:44:21:60:02");
        config.setPrivateKeyContent(UnitTestConstants.PRIVATE_KEY);
        config.setClientEncryptionEnabled(true);
        config.setEncryptionKeyId("test-key-1");
        config.setEncryptionAuthenticationMode(EncryptionAuthenticationMode.DEFAULT_MODE);
        config.setPermitUnencryptedDownloads(false);
        config.setEncryptionPrivateKeyBytes(keyBytes);
        config.setEncryptionAlgorithm(AesGcmCipherDetails.INSTANCE_128_BIT.getCipherId());

        ConfigContext.validate(config);
    }

    public void canValidateTimeoutValues() throws Exception {
        final StandardConfigContext config = new StandardConfigContext();

        config.setMantaURL(DefaultsConfigContext.DEFAULT_MANTA_URL);
        config.setMantaUser("username");
        config.setMantaKeyId(UnitTestConstants.FINGERPRINT);
        config.setPrivateKeyContent(UnitTestConstants.PRIVATE_KEY);

        ConfigContext.validate(config);

        // setTimeout

        config.setTimeout(DefaultsConfigContext.DEFAULT_HTTP_TIMEOUT);
        ConfigContext.validate(config);

        // setTcpSocketTimeout

        config.setTcpSocketTimeout(DefaultsConfigContext.DEFAULT_TCP_SOCKET_TIMEOUT);
        ConfigContext.validate(config);

        // setConnectionRequestTimeout

        config.setConnectionRequestTimeout(DefaultsConfigContext.DEFAULT_CONNECTION_REQUEST_TIMEOUT);
        ConfigContext.validate(config);

        // setExpectContinueTimeout

        config.setExpectContinueTimeout(-1);
        Assert.assertThrows(ConfigurationException.class, () ->
            ConfigContext.validate(config));

        config.setExpectContinueTimeout(DefaultsConfigContext.DEFAULT_EXPECT_CONTINUE_TIMEOUT);
        ConfigContext.validate(config);

        config.setExpectContinueTimeout(HttpRequestExecutor.DEFAULT_WAIT_FOR_CONTINUE);
        ConfigContext.validate(config);

    }
}
