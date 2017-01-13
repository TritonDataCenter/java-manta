package com.joyent.manta.config;

import org.apache.commons.codec.Charsets;
import org.apache.commons.lang3.BooleanUtils;

/**
 * {@link ConfigContext} implementation that loads
 * configuration parameters in an order that makes sense for unit testing
 * and allows for TestNG parameters to be loaded.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class IntegrationTestConfigContext extends SystemSettingsConfigContext {
    /**
     * Populate configuration from defaults, environment variables, system
     * properties and an addition context passed in.
     */
    public IntegrationTestConfigContext() {
        super(enableTestEncryption(new StandardConfigContext(),  encryptionEnabled()));
    }

    /**
     * Populate configuration from defaults, environment variables, system
     * properties and an addition context passed in. Assigns hard-coded
     * client-side encryption configuration settings.
     */
    public IntegrationTestConfigContext(Boolean usingEncryption) {
        super(enableTestEncryption(new StandardConfigContext(),
                encryptionEnabled() || BooleanUtils.isNotFalse(usingEncryption)));
    }

    private static <T> SettableConfigContext<T> enableTestEncryption(
            final SettableConfigContext<T> context,
            final boolean usingEncryption) {
        if (usingEncryption) {
            context.setClientEncryptionEnabled(true);
            context.setEncryptionKeyId("integration-test-key");
            context.setEncryptionAlgorithm("AES/GCM/NoPadding");
            context.setEncryptionPrivateKeyBytes("FFFFFFFBD96783C6C91E2222"
                    .getBytes(Charsets.US_ASCII));
        }

        return context;
    }

    public static boolean encryptionEnabled() {
        String sysProp = System.getProperty(MapConfigContext.MANTA_CLIENT_ENCRYPTION_ENABLED_KEY);
        String envVar = System.getenv(EnvVarConfigContext.MANTA_CLIENT_ENCRYPTION_ENABLED_ENV_KEY);

        return BooleanUtils.toBoolean(sysProp) || BooleanUtils.toBoolean(envVar);
    }
}
