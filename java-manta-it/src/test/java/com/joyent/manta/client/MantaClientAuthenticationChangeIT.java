/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.client.crypto.ExternalSecurityProviderLoader;
import com.joyent.manta.config.AuthAwareConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import com.joyent.manta.config.StandardConfigContext;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.Validate;
import org.bouncycastle.openssl.jcajce.JcaMiscPEMGenerator;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.openssl.jcajce.JcePEMEncryptorBuilder;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Test
public class MantaClientAuthenticationChangeIT {

    private AuthAwareConfigContext config;

    private MantaClient mantaClient;

    private String testPathPrefix;

    private StandardConfigContext backupConfig;

    @BeforeClass
    public void beforeClass() throws IOException {
        // Let TestNG configuration take precedence over environment variables
        config = new AuthAwareConfigContext(new IntegrationTestConfigContext());
        mantaClient = new MantaClient(config);
        testPathPrefix = IntegrationTestConfigContext.generateBasePath(config, this.getClass().getSimpleName());

        // stash authentication parameters so we can restore them between test methods
        backupConfig = new StandardConfigContext();
        backupConfig.setMantaKeyId(config.getMantaKeyId());
        backupConfig.setPassword(config.getPassword());

        if (config.getPrivateKeyContent() != null) {
            backupConfig.setPrivateKeyContent(config.getPrivateKeyContent());
        } else {
            backupConfig.setMantaKeyPath(config.getMantaKeyPath());
        }

        mantaClient.putDirectory(testPathPrefix, true);
    }

    @BeforeMethod
    public void beforeMethod() throws IOException {
        // restore user-provided authentication config since we might be switching between passwordless/passworded and path-based/content-based

        config.setMantaKeyId(backupConfig.getMantaKeyId());
        config.setPassword(backupConfig.getPassword());

        if (config.getMantaKeyPath() != null) {
            config.setMantaKeyPath(null);
        }

        if (config.getPrivateKeyContent() != null) {
            config.setPrivateKeyContent(null);
        }

        if (backupConfig.getPrivateKeyContent() != null) {
            config.setPrivateKeyContent(backupConfig.getPrivateKeyContent());
        } else {
            config.setMantaKeyPath(backupConfig.getMantaKeyPath());
        }

        config.reload();
    }

    @AfterClass
    public void afterClass() throws Exception {
        IntegrationTestConfigContext.cleanupTestDirectory(mantaClient, testPathPrefix);
        config.close();
    }

    public void canDisableAuthAndStillAccessPublicObjects() throws Exception {
        final String home = config.getMantaHomeDirectory();
        Assert.assertNotNull(config.getKeyPair());

        final List<String> homeListing = mantaClient.listObjects(home)
                .map(MantaObject::getPath)
                .collect(Collectors.toList());

        Assert.assertTrue(homeListing.contains(home + "/stor"));
        Assert.assertTrue(homeListing.contains(home + "/public"));

        config.setNoAuth(true);
        config.reload();

        Assert.assertNull(config.getKeyPair());

        mantaClient.head(home + "/public");

        final MantaClientHttpResponseException forbidden = Assert.expectThrows(
                MantaClientHttpResponseException.class,
                () -> mantaClient.head(home + "/stor"));

        Assert.assertEquals(403, forbidden.getStatusCode());

        config.setNoAuth(false);
        config.reload();

        mantaClient.head(home + "/stor");
    }

    public void canMigrateKeyAndContinueAccessingPrivateObjects() throws Exception {
        final boolean usingPath = config.getMantaKeyPath() != null;
        final KeyPair initialKeyPair = config.getKeyPair();

        final String testFile = testPathPrefix + UUID.randomUUID();
        final byte[] testContent = RandomUtils.nextBytes(RandomUtils.nextInt(500, 1500));

        mantaClient.put(testFile, testContent);

        // swap key location and make sure a new KeyPair is being used
        swapKeyLocation(config, usingPath);
        config.reload();
        final KeyPair swappedLocationKeyPair = config.getKeyPair();
        Assert.assertNotNull(swappedLocationKeyPair);
        Assert.assertNotSame(initialKeyPair, swappedLocationKeyPair);

        final byte[] retrievedContent = new byte[testContent.length];
        IOUtils.read(mantaClient.getAsInputStream(testFile), retrievedContent);
        AssertJUnit.assertArrayEquals(testContent, retrievedContent);
    }

    public void canTogglePasswordOnKeyAndContinueAccessingPrivateObjects() throws Exception {
        final boolean usingPassword = config.getPassword() != null;
        final KeyPair initialKeyPair = config.getKeyPair();

        final String testFile = testPathPrefix + UUID.randomUUID();
        final byte[] testContent = RandomUtils.nextBytes(RandomUtils.nextInt(500, 1500));

        // sent data to Manta
        mantaClient.put(testFile, testContent);

        // move the key to key content if it's path-based so we don't serialize a password-protected key without its password
        if (config.getMantaKeyPath() != null) {
            swapKeyLocation(config, true);
        }

        if (usingPassword) {
            swapKeyContentPasswordness(config, null);
        } else {
            final String randomPassword = UUID.randomUUID().toString();
            swapKeyContentPasswordness(config, randomPassword);
        }

        // swapKeyLocation(config, false);

        config.reload();
        final KeyPair swappedLocationKeyPair = config.getKeyPair();
        Assert.assertNotNull(swappedLocationKeyPair);
        Assert.assertNotSame(initialKeyPair, swappedLocationKeyPair);

        final byte[] retrievedContent = new byte[testContent.length];
        IOUtils.read(mantaClient.getAsInputStream(testFile), retrievedContent);
        AssertJUnit.assertArrayEquals(testContent, retrievedContent);
    }

    private static void swapKeyLocation(final AuthAwareConfigContext config, final boolean fromPathToContent) throws IOException {

        if (fromPathToContent) {
            // move key to MANTA_KEY_CONTENT
            final String keyContent = FileUtils.readFileToString(new File(config.getMantaKeyPath()), StandardCharsets.UTF_8);
            config.setMantaKeyPath(null);
            config.setPrivateKeyContent(keyContent);
            return;
        }

        // move key to MANTA_KEY_FILE
        final Path tempKey = Paths.get("/Users/tomascelaya/sandbox/");
        FileUtils.forceDeleteOnExit(tempKey.toFile());
        FileUtils.writeStringToFile(tempKey.toFile(), config.getPrivateKeyContent(), StandardCharsets.UTF_8);
        config.setPrivateKeyContent(null);
        config.setMantaKeyPath(tempKey.toString());
    }

    private static void swapKeyContentPasswordness(final AuthAwareConfigContext config, final String password) throws IOException, NoSuchProviderException, NoSuchAlgorithmException {
        Validate.isTrue(config.getMantaKeyPath() == null, "Key path should be null when toggling key content password");
        Validate.notBlank(config.getPrivateKeyContent(), "Key content should not be null");

        if (password == null) {
            Validate.notNull(config.getPassword(), "Password removal requested but no password attached");

            // removing password
            throw new AssertionError("Not yet implemented");
        }

        // adding password

        // make sure the KeyPair is loaded before we try to serialize it with the provided password
        Assert.assertNotNull(config.getKeyPair());

        final String keyAlgo = config.getKeyPair().getPrivate().getAlgorithm();

        // we can only reliably password-protect a keypair if libnss is disabled OR it's an RSA key, otherwise just skip
        if (ExternalSecurityProviderLoader.getPkcs11Provider() == null || keyAlgo.equals("RSA")) {
            try (final StringWriter contentWriter = new StringWriter();
                 final JcaPEMWriter pemWriter = new JcaPEMWriter(contentWriter)) {

                final JcaMiscPEMGenerator keySerializer = new JcaMiscPEMGenerator(
                        config.getKeyPair().getPrivate(),
                        new JcePEMEncryptorBuilder("AES-128-CBC")
                                .setProvider("BC")
                                .build(password.toCharArray()));

                pemWriter.writeObject(keySerializer);
                pemWriter.flush();

                config.setPrivateKeyContent(contentWriter.getBuffer().toString());
            }
        } else {
            throw new SkipException(
                    String.format("Unsupported parameters for attaching passphrase: libnss enabled %s, key algorithm: %s",
                            ExternalSecurityProviderLoader.getPkcs11Provider() != null,
                            keyAlgo));
        }

        config.setPassword(password);
    }
}
