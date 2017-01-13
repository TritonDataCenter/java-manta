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
        this(false);
    }

    /**
     * Populate configuration from defaults, environment variables, system
     * properties and an addition context passed in. Assigns hard-coded
     * client-side encryption configuration settings.
     */
    public IntegrationTestConfigContext(Boolean usingEncryption) {
        super();

        if (BooleanUtils.isTrue(usingEncryption)) {
            setClientEncryptionEnabled(true);
            setEncryptionKeyId("integration-test-key");
            setEncryptionAlgorithm("AES/GCM/NoPadding");
            setEncryptionPrivateKeyBytes("FFFFFFFBD96783C6C91E2222"
                    .getBytes(Charsets.US_ASCII));
        }
    }
}
