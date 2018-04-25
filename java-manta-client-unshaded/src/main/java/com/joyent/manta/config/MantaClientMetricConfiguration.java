package com.joyent.manta.config;

import com.codahale.metrics.MetricRegistry;

import java.util.UUID;

import static org.apache.commons.lang3.Validate.inclusiveBetween;
import static org.apache.commons.lang3.Validate.notNull;

public final class MantaClientMetricConfiguration {

    private final UUID clientId;
    private final MetricRegistry registry;
    private final MetricReporterMode reporterMode;
    private final Integer periodicReporterOutputInterval;

    /**
     * Empty configuration indicating no metrics should be collected.
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
     * @param registry supplied MetricRegistry to use for verification
     */
    public MantaClientMetricConfiguration(final UUID clientId, final MetricRegistry registry) {
        this.clientId = notNull(clientId);
        this.registry = notNull(registry);
        this.reporterMode = null;
        this.periodicReporterOutputInterval = null;
    }

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

}
