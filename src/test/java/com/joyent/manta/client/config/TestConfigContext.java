package com.joyent.manta.client.config;

import com.joyent.manta.config.*;

import java.net.URL;
import java.util.Properties;

/**
 * {@link com.joyent.manta.config.ConfigContext} implementation that loads
 * configuration parameters in an order that makes sense for unit testing
 * and allows for TestNG parameters to be loaded.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class TestConfigContext extends BaseChainedConfigContext {
    /**
     * Populate configuration from defaults, environment variables, system
     * properties and an addition context passed in.
     *
     * @param context additional context to layer on top
     * @param properties properties to load into context
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
     * @param context additional context to layer on top
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
                mantaKeyId, mantaTimeout));
    }

    static ConfigContext buildTestContext(String mantaUrl,
                                          String mantaUser,
                                          String mantaKeyPath,
                                          String mantaKeyId,
                                          Integer mantaTimeout) {
        URL privateKeyUrl = mantaKeyPath == null ?
                null :
                Thread.currentThread().getContextClassLoader().getResource(mantaKeyPath);

        BaseChainedConfigContext testConfig = new StandardConfigContext()
                .setMantaURL(mantaUrl)
                .setMantaUser(mantaUser)
                .setMantaKeyId(mantaKeyId)
                .setTimeout(mantaTimeout);

        if (privateKeyUrl != null) {
            testConfig.setMantaKeyPath(privateKeyUrl.getFile());
        } else {
            testConfig.setMantaKeyPath(mantaKeyPath);
        }

        return testConfig;
    }
}
