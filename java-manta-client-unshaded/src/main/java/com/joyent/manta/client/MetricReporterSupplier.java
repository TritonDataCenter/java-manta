/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.jmx.JmxReporter;
import com.joyent.manta.config.MantaClientMetricConfiguration;
import com.joyent.manta.exception.MantaClientException;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import static org.apache.commons.lang3.Validate.notNull;

/**
 * Helper class for building and starting a metric reporter given a {@link MantaClientMetricConfiguration}.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 * @author <a href="https://github.com/nairashwin952013">Ashwin A Nair</a>
 */
final class MetricReporterSupplier implements Supplier<Closeable> {

    /**
     * The logger name to use for SLF4J reporting.
     */
    public static final String FMT_METRIC_LOGGER_NAME = "com.joyent.manta.client.metrics";

    /**
     * A metric reporter constructed and configured with settings from the supplied configuration.
     */
    private final Closeable reporter;

    /**
     * Constructing an instance of this class prepares the desired metric reporter based on the supplied configuration.
     *
     * @param metricConfig details about the type of reporter to construct and add any necessary additional parameters
     */
    MetricReporterSupplier(final MantaClientMetricConfiguration metricConfig) {
        notNull(metricConfig);

        switch (metricConfig.getReporterMode()) {
            case JMX:
                final JmxReporter jmxReporter = buildJmxReporter(metricConfig);
                jmxReporter.start();
                this.reporter = jmxReporter;
                break;
            case SLF4J:
                final Slf4jReporter slf4jReporter = buildSlf4jReporter(metricConfig);
                slf4jReporter.start(metricConfig.getPeriodicReporterOutputInterval(), TimeUnit.SECONDS);
                this.reporter = slf4jReporter;
                break;
            case DISABLED:
            default:
                this.reporter = null;
        }
    }

    @Override
    public Closeable get() {
        return this.reporter;
    }

    /**
     * Builds a SLF4J metric reporter based on the supplied configuration. It is mandatory to define a reporter interval
     * if we opt for this reporter mode and it is implemented by leveraging the configuration parameter
     * "manta.metric_reporter.output_interval"
     *
     * @param metricConfig details about the type of reporter to construct and add any necessary additional parameters
     */
    private static Slf4jReporter buildSlf4jReporter(final MantaClientMetricConfiguration metricConfig) {
        return Slf4jReporter.forRegistry(metricConfig.getRegistry())
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .prefixedWith(metricConfig.getClientId().toString())
                .outputTo(LoggerFactory.getLogger(FMT_METRIC_LOGGER_NAME))
                .build();
    }

    /**
     * Builds a JMX metric reporter based on the supplied configuration. We create additional registries
     * `ConfigContextMBean` and 'retries' which are used to track all configuration information associated with a
     * specific client UUID. Running manta-monitor helps us in verifying all JMX and configuration metrics that have
     * been exposed.
     *
     * @param metricConfig details about the type of reporter to construct and add any necessary additional parameters
     */
    private static JmxReporter buildJmxReporter(final MantaClientMetricConfiguration metricConfig) {
        return JmxReporter.forRegistry(metricConfig.getRegistry())
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .createsObjectNamesWith(
                        (type, domain, name) -> {
                            final String metricJmxObjectName = String.format(
                                    MantaClientAgent.FMT_MBEAN_OBJECT_NAME,
                                    name,
                                    metricConfig.getClientId());

                            try {
                                return new ObjectName(metricJmxObjectName);
                            } catch (final MalformedObjectNameException e) {
                                final String msg = String.format(
                                        "Unable to create JMX object name for metric: %s (name=%s, clientId=%s)",
                                        metricJmxObjectName,
                                        name,
                                        metricConfig.getClientId());
                                throw new MantaClientException(msg, e);
                            }
                        }
                )
                .build();
    }
}
