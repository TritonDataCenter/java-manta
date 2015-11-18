/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.config;

/**
 * Configuration context that is entirely driven by in-memory parameters.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class StandardConfigContext extends BaseChainedConfigContext {
    /**
     * Creates a new {@link ConfigContext} implementation that allows for
     * programmatic configuration.
     */
    public StandardConfigContext() {
        super();
    }
}
