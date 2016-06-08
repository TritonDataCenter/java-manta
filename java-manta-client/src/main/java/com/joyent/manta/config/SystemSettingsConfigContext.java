/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.config;

import java.util.Properties;

/**
 * Implementation of {@link ConfigContext} that inherits from defaults,
 * environment variables and from Java system properties.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class SystemSettingsConfigContext extends BaseChainedConfigContext {
    /**
     * Populate configuration from defaults, environment variables and system
     * properties.
     */
    public SystemSettingsConfigContext() {
        this(true, System.getProperties());
    }

    /**
     * Populate configuration from defaults, environment variables and system
     * properties.
     * @param properties properties to load into context
     * @param includeEnvironmentVars flag indicated if we include the environment into the context
     */
    public SystemSettingsConfigContext(final boolean includeEnvironmentVars,
                                       final Properties properties) {
        super();
        // load defaults
        overwriteWithContext(DEFAULT_CONFIG);

        // overwrite with system properties
        MapConfigContext mapConfig = new MapConfigContext(properties);

        /* This is a workaround that allows us to provide a default key path and
         * to allow the override of a key path based configuration with
         * an embedded key. */
        if (isPresent(mapConfig.getPrivateKeyContent())) {
            setMantaKeyPath(null);
        }

        overwriteWithContext(mapConfig);

        if (includeEnvironmentVars) {
            EnvVarConfigContext envConfig = new EnvVarConfigContext();

            /* This is the same workaround as above, but for environment variables.
             * This checks to see if we have set private key contents and haven't
             * explicitly set the key path outside of the defaults. */
            if (!isPresent(mapConfig.getMantaKeyPath()) &&
                    isPresent(envConfig.getPrivateKeyContent())) {
                setMantaKeyPath(null);
            }

            overwriteWithContext(envConfig);
        }
    }

    /**
     * Populate configuration from defaults, environment variables, system
     * properties and an addition context passed in.
     *
     * @param context additional context to layer on top
     */
    public SystemSettingsConfigContext(final ConfigContext context) {
        // load all of the chained defaults
        this();
        // now load in an additional context
        overwriteWithContext(context);
    }
}
