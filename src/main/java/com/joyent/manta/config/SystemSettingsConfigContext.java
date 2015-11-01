/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.config;

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
        // load defaults
        super();
        // overwrite with system properties
        overwriteWithContext(new MapConfigContext(System.getProperties()));
        overwriteWithContext(new EnvVarConfigContext());
    }

    /**
     * Populate configuration from defaults, environment variables, system
     * properties and an addition context passed in.
     *
     * @param context additional context to layer on top
     */
    public SystemSettingsConfigContext(ConfigContext context) {
        // load all of the chained defaults
        this();
        // now load in an additional context
        overwriteWithContext(context);
    }
}
