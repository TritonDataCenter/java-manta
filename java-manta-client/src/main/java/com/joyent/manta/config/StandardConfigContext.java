/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
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
