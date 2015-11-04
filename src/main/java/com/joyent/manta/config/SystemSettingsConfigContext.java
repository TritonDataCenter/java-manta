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
     */
    public SystemSettingsConfigContext(final ConfigContext context) {
        // load all of the chained defaults
        this();
        // now load in an additional context
        overwriteWithContext(context);
    }
}
