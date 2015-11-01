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
    public ChainedConfigContext(ConfigContext... contexts) {
        for (ConfigContext c : contexts) {
            overwriteWithContext(c);
        }
    }
}
