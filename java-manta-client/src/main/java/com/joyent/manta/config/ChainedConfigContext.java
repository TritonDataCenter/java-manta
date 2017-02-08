/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
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
     * overwrite the values of the previous contexts (but never overwriting
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
