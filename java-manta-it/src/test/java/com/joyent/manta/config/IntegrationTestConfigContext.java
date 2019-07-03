/*
 * Copyright (c) 2015-2019, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.config;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.crypto.SupportedCiphersLookupMap;
import com.joyent.manta.util.MantaUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.ObjectUtils;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.util.Base64;
import java.util.UUID;

import static com.joyent.manta.client.MantaClient.SEPARATOR;

/**
 * {@link ConfigContext} implementation that loads
 * configuration parameters in an order that makes sense for unit testing
 * and allows for TestNG parameters to be loaded.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @author <a href="https://github.com/nairashwin952013">Ashwin A Nair</a>
 */
public class IntegrationTestConfigContext extends SystemSettingsConfigContext {

    private static String suiteRunId = UUID.randomUUID().toString();

    /**
     * Populate configuration from defaults, environment variables, system
     * properties and an addition context passed in.
     */
    public IntegrationTestConfigContext() {
        super(enableTestEncryption(new StandardConfigContext(), encryptionEnabled(), encryptionCipher()));
    }

    /**
     * Populate configuration from defaults, environment variables, system
     * properties and an addition context passed in. Assigns hard-coded
     * client-side encryption configuration settings.
     */
    public IntegrationTestConfigContext(Boolean usingEncryption) {
        super(enableTestEncryption(new StandardConfigContext(),
                (encryptionEnabled() && usingEncryption == null) ||
                        BooleanUtils.isTrue(usingEncryption), encryptionCipher()));
    }

    /**
     * Populate configuration from defaults, environment variables, system
     * properties and an addition context passed in. Assigns hard-coded
     * client-side encryption cipher algorithm settings.
     */
    public IntegrationTestConfigContext(final String encryptionCipher) {
        this(encryptionCipher != null, encryptionCipher);
    }

    /**
     * Populate configuration from defaults, environment variables, system
     * properties and an addition context passed in. Assigns hard-coded
     * client-side encryption configuration settings.
     */
    public IntegrationTestConfigContext(Boolean usingEncryption, String encryptionCipher) {
        super(enableTestEncryption(new StandardConfigContext(),
                (encryptionEnabled() && usingEncryption == null) ||
                        BooleanUtils.isTrue(usingEncryption), encryptionCipher));
    }

    private static <T> SettableConfigContext<T> enableTestEncryption(
            final SettableConfigContext<T> context,
            final boolean usingEncryption,
            final String encryptionCipher) {
        if (usingEncryption) {
            context.setClientEncryptionEnabled(true);
            context.setEncryptionKeyId("integration-test-key");

            EncryptionAuthenticationMode envSettingsMode = MantaUtils.parseEnumOrNull(
                    encryptionAuthenticationMode(), EncryptionAuthenticationMode.class);

            context.setEncryptionAuthenticationMode(ObjectUtils.firstNonNull(
                    envSettingsMode, EncryptionAuthenticationMode.Optional));

            SupportedCipherDetails cipherDetails = SupportedCiphersLookupMap.INSTANCE.getOrDefault(encryptionCipher,
                    DefaultsConfigContext.DEFAULT_CIPHER);

            context.setEncryptionAlgorithm(cipherDetails.getCipherId());
            SecretKey key = SecretKeyUtils.generate(cipherDetails);
            context.setEncryptionPrivateKeyBytes(key.getEncoded());

            System.out.printf(
                    "Integration test encryption enabled: %s\n"
                            + "Unique secret key used for test (base64):\n%s\n",
                    cipherDetails.getCipherId(),
                    Base64.getEncoder().encodeToString(key.getEncoded()));
        }

        return context;
    }

    public static boolean encryptionEnabled() {
        String sysProp = System.getProperty(MapConfigContext.MANTA_CLIENT_ENCRYPTION_ENABLED_KEY);
        String envVar = System.getenv(EnvVarConfigContext.MANTA_CLIENT_ENCRYPTION_ENABLED_ENV_KEY);

        return BooleanUtils.toBoolean(sysProp) || BooleanUtils.toBoolean(envVar);
    }

    public static String encryptionCipher() {
        String sysProp = System.getProperty(MapConfigContext.MANTA_ENCRYPTION_ALGORITHM_KEY);
        String envVar = System.getenv(EnvVarConfigContext.MANTA_ENCRYPTION_ALGORITHM_ENV_KEY);

        return sysProp != null ? sysProp : envVar;
    }

    public static String encryptionAuthenticationMode() {
        String sysProp = System.getProperty(MapConfigContext.MANTA_ENCRYPTION_AUTHENTICATION_MODE_KEY);
        String envVar = System.getenv(EnvVarConfigContext.MANTA_ENCRYPTION_AUTHENTICATION_MODE_ENV_KEY);

        return sysProp != null ? sysProp : envVar;
    }

    public static String generateSuiteBasePath(final ConfigContext config) {
        final String integrationTestBase = ObjectUtils.firstNonNull(
                System.getenv("MANTA_IT_PATH"),
                System.getProperty("manta.it.path"),
                String.format("%s/stor/java-manta-integration-tests/%s/",
                              config.getMantaHomeDirectory(),
                              suiteRunId));
        return integrationTestBase;
    }


    public static String generateBasePath(final ConfigContext config, final String testBaseName) {
        return generateSuiteBasePath(config) + testBaseName + SEPARATOR;
    }

    public static String generateBasePathWithoutSeparator(final ConfigContext config, final String testBaseName) {
        return generateSuiteBasePath(config) + testBaseName;
    }

    public static void cleanupTestDirectory(final MantaClient mantaClient, final String testPathPrefix) throws IOException {
        final Boolean skipCleanup = Boolean.valueOf(
                ObjectUtils.firstNonNull(System.getenv("MANTA_IT_NO_CLEANUP"), System.getProperty("manta.it.no_cleanup")));
        if (!skipCleanup
                && mantaClient != null
                && mantaClient.head(testPathPrefix) != null) {
            mantaClient.deleteRecursive(testPathPrefix);
            mantaClient.closeWithWarning();
        }

    }

}
