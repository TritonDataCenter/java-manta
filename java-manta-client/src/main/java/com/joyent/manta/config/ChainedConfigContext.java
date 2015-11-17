/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.config;

/**
 * Implementation of {@link ConfigContext} that links together multiple contexts.
 * This allows you to create tiers of configuration in which certain configuration
 * contexts are given priority over others.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class ChainedConfigContext extends BaseChainedConfigContext {
    /**
     * Creates a new {@link ConfigContext} implementation that allows you
     * to chain together multiple configuration contexts that progressively
     * overwrite the values of the previous contexts (but never overwritting
     * with null).
     *
     * @param contexts N number of configuration contexts
     */
    public ChainedConfigContext(final ConfigContext... contexts) {
        for (ConfigContext c : contexts) {
            overwriteWithContext(c);
        }
    }
}
