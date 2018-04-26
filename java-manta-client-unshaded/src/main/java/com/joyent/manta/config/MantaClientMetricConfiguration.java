/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.config;

import com.codahale.metrics.MetricRegistry;

import java.util.UUID;

import static org.apache.commons.lang3.Validate.inclusiveBetween;
import static org.apache.commons.lang3.Validate.notNull;

/**
 * Value object describing how metrics should be exposed for a {@link com.joyent.manta.client.MantaClient}.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 * @since 3.1.9, 3.2.2
 */
public final class MantaClientMetricConfiguration {

    /**
     * Unique identifier for each {@link com.joyent.manta.client.MantaClient} instance. Used to avoid collisions when
     * registering objects in JMX.
     */
    private final UUID clientId;

    /**
     * Registry used to track metrics and to be used with a reporter (if one is set).
     */
    private final MetricRegistry registry;

    /**
     * Method for reporting metrics.
     */
    private final MetricReporterMode reporterMode;

    /**
     * Nullable duration used by periodic reporting modes.
     */
    private final Integer periodicReporterOutputInterval;

    /**
     * Empty configuration indicating no metrics should be collected. Ideally this constructor would be package-private
     * but it is needed for testing by {@link com.joyent.manta.client.MantaClientAgent}.
     */
    public MantaClientMetricConfiguration() {
        this.clientId = null;
        this.registry = null;
        this.reporterMode = null;
        this.periodicReporterOutputInterval = null;
    }

    /**
     * Allow users to provide an existing MetricRegistry they might want to reuse. Caller is responsible for
     * how reporting those metrics is handled.
     *
     * @param clientId client unique identifier
     * @param registry user-constructed metric registry
     */
    public MantaClientMetricConfiguration(final UUID clientId, final MetricRegistry registry) {
        this.clientId = notNull(clientId);
        this.registry = notNull(registry);
        this.reporterMode = null;
        this.periodicReporterOutputInterval = null;
    }

    /**
     * Construct a configuration for tracking metrics using the supplied registry, how those metrics should be reported,
     * and the periodic output interval required by certain reporting modes.
     *
     * @param clientId                       client unique identifier
     * @param registry                       user-constructed metric registry
     * @param reporterMode                   method for reporting metrics
     * @param periodicReporterOutputInterval potentially-null time for periodic metric reporters, validated for certain
     *                                       reporting modes
     * @throws NullPointerException     when the interval is required but none was given
     * @throws IllegalArgumentException when the interval is provided but invalid
     */
    public MantaClientMetricConfiguration(
            final UUID clientId,
            final MetricRegistry registry,
            final MetricReporterMode reporterMode,
            final Integer periodicReporterOutputInterval
    ) {
        this.clientId = notNull(clientId);
        this.registry = notNull(registry);
        this.reporterMode = notNull(reporterMode);
        this.periodicReporterOutputInterval = validateReporterOutputInterval(periodicReporterOutputInterval);
    }

    public UUID getClientId() {
        return this.clientId;
    }

    public MetricRegistry getRegistry() {
        return this.registry;
    }

    public MetricReporterMode getReporterMode() {
        return this.reporterMode;
    }

    public Integer getPeriodicReporterOutputInterval() {
        return this.periodicReporterOutputInterval;
    }

    /**
     * Check that the supplied reporter output interval is valid if the given reporting mode requires one and return it.
     *
     * @param reporterOutputInterval the interval to validate
     * @return the valid interval
     * @throws NullPointerException     when the interval is required but none was given
     * @throws IllegalArgumentException when the interval is provided but invalid
     */
    private Integer validateReporterOutputInterval(final Integer reporterOutputInterval) {
        if (!this.reporterMode.equals(MetricReporterMode.SLF4J)) {
            return reporterOutputInterval;
        }
        notNull(reporterOutputInterval,
                "Reporter output interval must be set when SLF4J reporter is selected");
        inclusiveBetween(1, Integer.MAX_VALUE, (Comparable<Integer>) reporterOutputInterval,
                "Reporter output interval must be greater than 0 when SLF4J reporter is selected");
        return reporterOutputInterval;
    }
}
