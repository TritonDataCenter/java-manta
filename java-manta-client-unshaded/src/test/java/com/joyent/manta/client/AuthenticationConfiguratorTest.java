/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.http.signature.ThreadLocalSigner;
import com.joyent.manta.config.BaseChainedConfigContext;
import com.joyent.manta.config.TestConfigContext;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.text.RandomStringGenerator;
import org.bouncycastle.openssl.jcajce.JcaMiscPEMGenerator;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.openssl.jcajce.JcePEMEncryptorBuilder;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;

@Test
public class AuthenticationConfiguratorTest {

    private static final RandomStringGenerator STRING_GENERATOR =
            new RandomStringGenerator.Builder()
                    .withinRange((int) 'a', (int) 'z')
                    .build();

    private BaseChainedConfigContext config;

    @BeforeMethod
    public void setUp() throws Exception {
        final ImmutablePair<KeyPair, BaseChainedConfigContext> keypairAndConfig = TestConfigContext.generateKeyPairBackedConfig();
        config = keypairAndConfig.right
                .setMantaURL("https://localhost")
                .setMantaUser("user");
    }

    @AfterMethod
    public void tearDown() throws Exception {
    }

    // PERHAPS: split this out into several tests since they're all pretty self-contained?
    public void canMonitorRelevantFieldsInConfig() throws IOException {
        final AuthenticationConfigurator authConfig = new AuthenticationConfigurator(config);
        final KeyPair currentKeyPair = authConfig.getKeyPair();
        Assert.assertNotNull(currentKeyPair);

        // failure to reload when encrypting the key with a password
        final String attachingPassphrase = STRING_GENERATOR.generate(16);
        try (final StringWriter contentWriter = new StringWriter();
             final JcaPEMWriter pemWriter = new JcaPEMWriter(contentWriter)) {
            final Object keySerializer = new JcaMiscPEMGenerator(
                    currentKeyPair.getPrivate(),
                    new JcePEMEncryptorBuilder("AES-128-CBC").build(attachingPassphrase.toCharArray()));
            pemWriter.writeObject(keySerializer);
            pemWriter.flush();
            config.setPrivateKeyContent(contentWriter.getBuffer().toString());
        }

        // password
        config.setPassword(attachingPassphrase);
        authConfig.reload();
        differentKeyPairsSameContent(currentKeyPair, authConfig.getKeyPair());

        // key file (move key content to a file)
        final File keyFile = File.createTempFile("private-key", "");
        FileUtils.forceDeleteOnExit(keyFile);
        FileUtils.writeStringToFile(keyFile, config.getPrivateKeyContent(), StandardCharsets.UTF_8);
        config.setPrivateKeyContent(null);
        config.setMantaKeyPath(keyFile.getAbsolutePath());
        differentKeyPairsSameContent(currentKeyPair, authConfig.getKeyPair());

        // key id
        config.setMantaKeyId("MD5:" + config.getMantaKeyId());
        authConfig.reload();
        differentKeyPairsSameContent(currentKeyPair, authConfig.getKeyPair());

        // disable native signatures
        final ThreadLocalSigner currentSigner = authConfig.getSigner();
        config.setDisableNativeSignatures(true);
        authConfig.reload();
        Assert.assertNotSame(currentSigner, authConfig.getSigner());

        // disable auth entirely
        config.setNoAuth(true);
        authConfig.reload();
        Assert.assertNull(authConfig.getKeyPair());
    }

    // TEST UTILITY METHODS

    private void differentKeyPairsSameContent(final KeyPair keyPairFromContent, final KeyPair keyPairFromFile) {
        // different instances
        Assert.assertNotSame(keyPairFromContent, keyPairFromFile);

        // same key
        AssertJUnit.assertArrayEquals(
                keyPairFromContent.getPrivate().getEncoded(),
                keyPairFromFile.getPrivate().getEncoded());
        AssertJUnit.assertArrayEquals(
                keyPairFromContent.getPublic().getEncoded(),
                keyPairFromFile.getPublic().getEncoded());
    }
}
