/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.config.ConfigContext;
import org.apache.commons.lang3.Validate;

import java.util.concurrent.ForkJoinPool;

/**
 * Factory class that returns a {@link ForkJoinPool} instance configured with
 * the proper number of maximum threads.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.1.7
 */
final class FindForkJoinPoolFactory {
    /**
     * System property indicating the amount of parallelism to set for the
     * default {@link ForkJoinPool}.
     */
    private static final String SYSTEM_FORK_JOIN_POOL_PARALLELISM_KEY =
            "java.util.concurrent.ForkJoinPool.common.parallelism";

    /**
     * Factory class with no need for instances.
     */
    private FindForkJoinPoolFactory() {
    }

    /**
     * Returns a new instance of {@link ForkJoinPool} configured with the
     * correct parallelism value.
     *
     * @param config Manta configuration context object
     * @return configured instance
     */
    static ForkJoinPool getInstance(final ConfigContext config) {
        Validate.notNull(config, "Configuration context is null");

        int processors = Runtime.getRuntime().availableProcessors();

        // Overwrite the default if the user has explicitly set a parallelism value
        final String parallelismSysProp = System.getProperty(SYSTEM_FORK_JOIN_POOL_PARALLELISM_KEY);
        if (parallelismSysProp != null) {
            try {
                processors = Integer.valueOf(System.getProperty(SYSTEM_FORK_JOIN_POOL_PARALLELISM_KEY));
            } catch (NumberFormatException e) {
                // Do nothing and let the default value stand
            }
        }

        /* We choose the small of the maximum number of connections minus two or
         * the number of available processors as the size of our fork join pool
         * for find() parallelism. */
        final int parallelism = Math.min(config.getMaximumConnections() - 2,
                processors);

        return new ForkJoinPool(parallelism);
    }
}
