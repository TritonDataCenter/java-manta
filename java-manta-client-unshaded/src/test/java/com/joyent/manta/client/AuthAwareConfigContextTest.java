/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.http.signature.KeyFingerprinter;
import com.joyent.http.signature.ThreadLocalSigner;
import com.joyent.manta.config.AuthAwareConfigContext;
import com.joyent.manta.config.BaseChainedConfigContext;
import com.joyent.manta.config.TestConfigContext;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;

@Test
public class AuthAwareConfigContextTest {
    private BaseChainedConfigContext config;

    @BeforeMethod
    public void setUp() {
        final ImmutablePair<KeyPair, BaseChainedConfigContext> keypairAndConfig = TestConfigContext.generateKeyPairBackedConfig();
        config = keypairAndConfig.right
                .setMantaURL("https://localhost")
                .setMantaUser("user");
    }

    // PERHAPS: split this out into several tests since they're all pretty self-contained?
    public void canMonitorRelevantFieldsInConfig() throws IOException {
        final AuthAwareConfigContext authConfig = new AuthAwareConfigContext(config);
        final KeyPair currentKeyPair = authConfig.getKeyPair();
        Assert.assertNotNull(currentKeyPair);

        // key file (move key content to a file)
        final File keyFile = File.createTempFile("private-key", "");
        FileUtils.forceDeleteOnExit(keyFile);
        FileUtils.writeStringToFile(keyFile, authConfig.getPrivateKeyContent(), StandardCharsets.UTF_8);
        authConfig.setPrivateKeyContent(null);
        authConfig.setMantaKeyPath(keyFile.getAbsolutePath());
        authConfig.reload();
        differentKeyPairsSameContent(currentKeyPair, authConfig.getKeyPair());

        // key id
        authConfig.setMantaKeyId("MD5:" + KeyFingerprinter.md5Fingerprint(authConfig.getKeyPair()));
        authConfig.reload();
        differentKeyPairsSameContent(currentKeyPair, authConfig.getKeyPair());

        // disable native signatures
        final ThreadLocalSigner currentSigner = authConfig.getSigner();
        authConfig.setDisableNativeSignatures(true);
        authConfig.reload();
        Assert.assertNotSame(currentSigner, authConfig.getSigner());

        // disable auth entirely
        authConfig.setNoAuth(true);
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
