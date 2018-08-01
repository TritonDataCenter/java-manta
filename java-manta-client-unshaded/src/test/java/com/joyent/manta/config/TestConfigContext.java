/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.config;

import com.joyent.http.signature.KeyFingerprinter;
import com.joyent.manta.util.UnitTestConstants;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.bouncycastle.openssl.jcajce.JcaMiscPEMGenerator;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.openssl.jcajce.JcePEMEncryptorBuilder;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import static com.joyent.manta.util.UnitTestConstants.UNIT_TEST_URL;

/**
 * {@link com.joyent.manta.config.ConfigContext} implementation that loads
 * configuration parameters in an order that makes sense for unit testing
 * and allows for TestNG parameters to be loaded.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class TestConfigContext extends BaseChainedConfigContext {

    public TestConfigContext() {
        super(new StandardConfigContext()
                .setMaximumConnections(DEFAULT_CONFIG.getMaximumConnections())
                .setRetries(DEFAULT_CONFIG.getRetries())
                .setHttpsProtocols(DEFAULT_CONFIG.getHttpsProtocols())
                .setHttpsCipherSuites(DEFAULT_CONFIG.getHttpsCipherSuites())
                .setMantaURL(UNIT_TEST_URL)
                .setMantaUser("username")
                .setMantaKeyId(UnitTestConstants.FINGERPRINT)
                .setPrivateKeyContent(UnitTestConstants.PRIVATE_KEY)
        );
    }

    /**
     * Populate configuration from defaults, environment variables, system
     * properties and an addition context passed in.
     *
     * @param context                additional context to layer on top
     * @param properties             properties to load into context
     * @param includeEnvironmentVars flag indicated if we include the environment into the context
     */
    public TestConfigContext(ConfigContext context, Properties properties,
                             boolean includeEnvironmentVars) {
        super();

        // load defaults
        overwriteWithContext(DEFAULT_CONFIG);
        // now load in an additional context
        overwriteWithContext(context);
        // overwrite with system properties
        overwriteWithContext(new MapConfigContext(properties));

        if (includeEnvironmentVars) {
            overwriteWithContext(new EnvVarConfigContext());
        }
    }

    /**
     * Populate configuration from defaults, environment variables, system
     * properties and an addition context passed in.
     *
     * @param context    additional context to layer on top
     * @param properties properties to load into context
     */
    public TestConfigContext(ConfigContext context, Properties properties) {
        this(context, properties, true);
    }

    /**
     * Populate configuration from defaults, environment variables, system
     * properties and an addition context passed in.
     *
     * @param context additional context to layer on top
     */
    public TestConfigContext(ConfigContext context) {
        this(context, System.getProperties());
    }

    public TestConfigContext(String mantaUrl,
                             String mantaUser,
                             String mantaKeyPath,
                             String mantaKeyId,
                             Integer mantaTimeout) {
        this(buildTestContext(mantaUrl, mantaUser, mantaKeyPath,
                mantaKeyId, mantaTimeout, 6));
    }

    static ConfigContext buildTestContext(String mantaUrl,
                                          String mantaUser,
                                          String mantaKeyPath,
                                          String mantaKeyId,
                                          Integer mantaTimeout,
                                          Integer retries) {
        URL privateKeyUrl = mantaKeyPath == null ?
                null :
                Thread.currentThread().getContextClassLoader().getResource(mantaKeyPath);

        BaseChainedConfigContext testConfig = new StandardConfigContext()
                .setMantaURL(mantaUrl)
                .setMantaUser(mantaUser)
                .setMantaKeyId(mantaKeyId)
                .setTimeout(mantaTimeout)
                .setRetries(retries);

        if (privateKeyUrl != null) {
            testConfig.setMantaKeyPath(privateKeyUrl.getFile());
        } else {
            testConfig.setMantaKeyPath(mantaKeyPath);
        }

        return testConfig;
    }

    public static ImmutablePair<KeyPair, BaseChainedConfigContext> generateKeyPairBackedConfig() {
        return generateKeyPairBackedConfig(null);
    }

    /**
     * Some test cases need a direct reference to a KeyPair along with it's associated config. Manually calling
     * KeyPairFactory with a half-baked config can get cumbersome, so let's build a ConfigContext which has
     * everything ready and supplies the relevant KeyPair.
     *
     * @return the generated keypair and a config which uses a serialized version of that keypair
     */
    public static ImmutablePair<KeyPair, BaseChainedConfigContext> generateKeyPairBackedConfig(final String passphrase) {
        final KeyPair keyPair;
        try {
            keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        } catch (final NoSuchAlgorithmException impossible) {
            throw new Error(impossible); // "RSA" is always provided
        }

        final Object keySerializer;
        if (passphrase != null) {
            try {
                keySerializer = new JcaMiscPEMGenerator(
                        keyPair.getPrivate(),
                        new JcePEMEncryptorBuilder("AES-128-CBC").build(passphrase.toCharArray()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            keySerializer = keyPair.getPrivate();
        }

        final String keyContent;
        try (final StringWriter content = new StringWriter();
             final JcaPEMWriter writer = new JcaPEMWriter(content)) {
            writer.writeObject(keySerializer);
            writer.flush();
            keyContent = content.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final BaseChainedConfigContext config = new ChainedConfigContext(DEFAULT_CONFIG)
                // we need to unset the key path in case one exists at ~/.ssh/id_rsa
                // see the static initializer in DefaultsConfigContext
                .setMantaKeyPath(null)
                .setPrivateKeyContent(keyContent)
                .setMantaKeyId(KeyFingerprinter.md5Fingerprint(keyPair));

        if (passphrase != null) {
            config.setPassword(passphrase);
        }

        return new ImmutablePair<>(keyPair, config);
    }
}
