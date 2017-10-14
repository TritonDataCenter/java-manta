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

import java.util.Optional;
import java.util.concurrent.ForkJoinPool;

/**
 * Factory class that returns a {@link ForkJoinPool} instance configured with
 * the with a maximum parallelism value based on Manta client settings.
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
     * Percent of maximum connections that are reserved so that they are not
     * used by the threads in the {@link ForkJoinPool}. We want a buffer
     * so that the {@link ForkJoinPool} doesn't use up all available
     * connections for the {@link MantaClient} instance.
     */
    private static final double PERCENT_OF_RESERVED_CONNECTIONS = 0.5;

    /**
     * Minimum number of connections to allow if the reserved
     * connection count is available as an fractional integer
     * of the maximum connection count.
     */
    private static final int MINIMUM_CONNECTIONS_VIABLE = 1;

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
        final int maximumConnections = Validate.notNull(
                config.getMaximumConnections(),
                "Maximum connections setting is null");

        Validate.isTrue(maximumConnections > 0,
                "Maximum connections is not greater than zero");

        final int parallelism = calculateParallelism(maximumConnections);

        return new ForkJoinPool(parallelism);
    }

    /**
     * Calculates the parallelism value for the {@link ForkJoinPool} by
     * comparing the maximum connections available in the HTTP connection
     * pool with the system parallelism setting.
     * @param maximumConnections maximum number of connections in the HTTP
     *                           connection pool
     * @return parallelism value
     */
    private static int calculateParallelism(final int maximumConnections) {
        final int reserved = calculateNumberOfReservedConnections(maximumConnections);

        final int maximumUsableConnections = Math.max(maximumConnections - reserved,
                MINIMUM_CONNECTIONS_VIABLE);

        /* If the maximum number of usable connections equals our minimum then
         * we are forced into a parallelism value of the minimum connections
         * value (ie 1). */
        if (maximumUsableConnections == MINIMUM_CONNECTIONS_VIABLE) {
            return MINIMUM_CONNECTIONS_VIABLE;
        }

        final int systemParallelism = calculateSystemParallelism();

        /* We choose from the lesser of the system parallelism value or the
         * number of connections available because: 1. We want to prevent
         * exhaustion of the HTTP connection pool when the ForkJoinPool
         * is running at its maximum thread count. 2. We don't want the
         * parallelism value to be higher than what the running system
         * supports. */
        return Math.min(maximumUsableConnections, systemParallelism);
    }

    /**
     * Calculates the number of connections to leave reserved so that the
     * {@link ForkJoinPool} can't allocate them.
     *
     * @param maximumConnections maximum number of connections in the HTTP
     *                           connection pool
     * @return fractional value rounded to the nearest integer of the number of
     *         connections to leave reserved
     */
    private static int calculateNumberOfReservedConnections(final int maximumConnections) {
        final double reserved = maximumConnections * PERCENT_OF_RESERVED_CONNECTIONS;
        return Math.toIntExact(Math.round(reserved));
    }

    /**
     * Calculates the system parallelism setting by choosing the default value
     * generated based on the number of processors or by choosing the user
     * supplied system property.
     *
     * @return integer representing the parallelism value for a {@link ForkJoinPool}
     */
    private static int calculateSystemParallelism() {
        final Optional<Integer> systemParallelism = readSystemForkJoinPoolParallelismSetting();
        return systemParallelism.orElse(Runtime.getRuntime().availableProcessors());
    }

    /**
     * Reads and parses the system {@link ForkJoinPool} system property if
     * present.
     *
     * @return optional integer value for the system parallelism setting
     */
    private static Optional<Integer> readSystemForkJoinPoolParallelismSetting() {
        final String parallelismSysProp = System.getProperty(SYSTEM_FORK_JOIN_POOL_PARALLELISM_KEY);

        if (parallelismSysProp == null) {
            return Optional.empty();
        }

        try {
            return Optional.of(Integer.valueOf(parallelismSysProp));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }
}
