package com.joyent.manta.config;

import java.net.URL;
import java.util.Properties;


/**
 * {@link ConfigContext} implementation that loads
 * configuration parameters in an order that makes sense for unit testing
 * and allows for TestNG parameters to be loaded.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class IntegrationTestConfigContext extends BaseChainedConfigContext {
    /**
     * Populate configuration from defaults, environment variables, system
     * properties and an addition context passed in.
     *
     * @param context additional context to layer on top
     * @param properties properties to load into context
     * @param includeEnvironmentVars flag indicated if we include the environment into the context
     */
    public IntegrationTestConfigContext(ConfigContext context, Properties properties,
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
    public IntegrationTestConfigContext(ConfigContext context, Properties properties) {
        this(context, properties, true);
    }

    /**
     * Populate configuration from defaults, environment variables, system
     * properties and an addition context passed in.
     *
     * @param context additional context to layer on top
     */
    public IntegrationTestConfigContext(ConfigContext context) {
        this(context, System.getProperties());
    }

    public IntegrationTestConfigContext(String mantaUrl,
                                        String mantaUser,
                                        String mantaKeyPath,
                                        String mantaKeyId,
                                        Integer mantaTimeout) {
        this(buildTestContext(mantaUrl, mantaUser, mantaKeyPath,
                mantaKeyId, mantaTimeout, 6));
    }

    public IntegrationTestConfigContext(String mantaUrl,
                                        String mantaUser,
                                        String mantaKeyPath,
                                        String mantaKeyId,
                                        Integer mantaTimeout,
                                        Integer retries) {
        this(buildTestContext(mantaUrl, mantaUser, mantaKeyPath,
                mantaKeyId, mantaTimeout, retries));
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
                .setMantaKeyId(mantaKeyId);

        if (mantaTimeout != null) {
            testConfig.setTimeout(mantaTimeout);
        }

        if (retries != null) {
            testConfig.setRetries(retries);
        }

        if (privateKeyUrl != null) {
            testConfig.setMantaKeyPath(privateKeyUrl.getFile());
        } else {
            testConfig.setMantaKeyPath(mantaKeyPath);
        }

        return testConfig;
    }
}
