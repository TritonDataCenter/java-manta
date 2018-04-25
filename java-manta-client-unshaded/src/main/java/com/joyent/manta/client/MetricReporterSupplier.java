package com.joyent.manta.client;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.jmx.JmxReporter;
import com.joyent.manta.config.MantaClientMetricConfiguration;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import static org.apache.commons.lang3.Validate.inclusiveBetween;
import static org.apache.commons.lang3.Validate.notNull;

final class MetricReporterSupplier implements Supplier<Closeable> {

    private final Closeable reporter;

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
                slf4jReporter.start(metricConfig.getPeriodicReporterOutputInterval(), TimeUnit.MILLISECONDS);
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

    private static Slf4jReporter buildSlf4jReporter(final MantaClientMetricConfiguration metricConfig) {
        return Slf4jReporter.forRegistry(metricConfig.getRegistry())
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .prefixedWith(metricConfig.getClientId().toString())
                .outputTo(LoggerFactory.getLogger("com.joyent.manta.client.metrics"))
                // .shutdownExecutorOnStop()
                // .markWith()
                // .withLoggingLevel()
                .build();
    }

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
                                throw new RuntimeException(msg, e);
                            }
                        }
                )
                .build();
    }
}
