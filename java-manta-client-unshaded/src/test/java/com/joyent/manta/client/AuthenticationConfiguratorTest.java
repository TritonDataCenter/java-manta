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
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
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

    public void canSkipReloadIfNothingRelevantChanges() throws IOException {
        final AuthenticationConfigurator authConfig = new AuthenticationConfigurator(config);
        final KeyPair keyPairBeforeReload = authConfig.getKeyPair();
        final ThreadLocalSigner signerBeforeReload = authConfig.getSigner();
        Assert.assertNotNull(keyPairBeforeReload);
        Assert.assertNotNull(signerBeforeReload);

        // change an unrelated setting
        config.setHttpBufferSize(1);

        // reload config, this should not do anything at all
        authConfig.reload();

        // derived objects unchanged
        Assert.assertSame(keyPairBeforeReload, authConfig.getKeyPair());
        Assert.assertSame(signerBeforeReload, authConfig.getSigner());
    }

    public void lacksFineGrainedReloading() throws IOException {
        // this test is here to document the fact that reloading is all-or-nothing
        final AuthenticationConfigurator authConfig = new AuthenticationConfigurator(config);
        final KeyPair keyPairBeforeReload = authConfig.getKeyPair();
        Assert.assertNotNull(keyPairBeforeReload);

        config.setMantaUser("admin");

        authConfig.reload();

        Assert.assertNotSame(keyPairBeforeReload, authConfig.getKeyPair());

        // same key
        AssertJUnit.assertArrayEquals(
                keyPairBeforeReload.getPrivate().getEncoded(),
                authConfig.getKeyPair().getPrivate().getEncoded());
    }

    // PERHAPS: split this out into several tests since they're all pretty self-contained?
    public void canMonitorRelevantFieldsInConfig() throws IOException {
        final AuthenticationConfigurator authConfig = new AuthenticationConfigurator(config);
        final KeyPair currentKeyPair = authConfig.getKeyPair();

        // username
        config.setMantaUser("newuser");
        authConfig.reload();
        differentKeyPairsSameContent(currentKeyPair, authConfig.getKeyPair());

        // failure to reload when encrypting the key with a password
        final String attachingPassphrase = RandomStringUtils.randomAlphanumeric(16);
        try (final StringWriter contentWriter = new StringWriter();
             final JcaPEMWriter pemWriter = new JcaPEMWriter(contentWriter)) {
            final Object keySerializer = new JcaMiscPEMGenerator(
                    currentKeyPair.getPrivate(),
                    new JcePEMEncryptorBuilder("AES-128-CBC").build(attachingPassphrase.toCharArray()));
            pemWriter.writeObject(keySerializer);
            pemWriter.flush();
            config.setPrivateKeyContent(contentWriter.getBuffer().toString());
        }

        final Throwable encryptedCastException = Assert.expectThrows(
                ClassCastException.class,
                authConfig::reload);

        Assert.assertTrue(
                encryptedCastException.getMessage().contains(
                        "PEMEncryptedKeyPair cannot be cast to org.bouncycastle.openssl.PEMKeyPair"));

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
        authConfig.reload();
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