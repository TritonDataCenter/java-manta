/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.jmx.JmxReporter;
import com.joyent.manta.config.MantaClientMetricConfiguration;
import com.joyent.manta.config.MetricReporterMode;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.lang.management.ManagementFactory;
import java.util.UUID;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import static com.joyent.manta.http.MantaHttpRequestRetryHandler.METRIC_NAME_RETRIES;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class MetricReporterSupplierTest {
    public void throwsOnNullInput() {
        Assert.assertThrows(NullPointerException.class, () -> new MetricReporterSupplier(null));
    }

    public void createsAndStartsJmxReporter() throws MalformedObjectNameException, InterruptedException {
        final UUID clientId = UUID.randomUUID();
        final MetricRegistry registry = new MetricRegistry();
        registry.meter(METRIC_NAME_RETRIES);

        final MantaClientMetricConfiguration metricConfig = new MantaClientMetricConfiguration(clientId, registry, MetricReporterMode.JMX, null);
        final Closeable jmxReporter = new MetricReporterSupplier(metricConfig).get();
        assertTrue(jmxReporter instanceof JmxReporter);

        final MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();

        assertTrue(platformMBeanServer.isRegistered(buildObjectName(clientId, METRIC_NAME_RETRIES)));

        final JmxReporter reporter = (JmxReporter) jmxReporter;
        reporter.close();

        assertFalse(platformMBeanServer.isRegistered(buildObjectName(clientId, METRIC_NAME_RETRIES)));
    }
    public void createsAndStartsSlf4jReporter() {
        final MantaClientMetricConfiguration metricConfig = new MantaClientMetricConfiguration(UUID.randomUUID(), new MetricRegistry(), MetricReporterMode.SLF4J, 1000);
        final Closeable jmxReporter = new MetricReporterSupplier(metricConfig).get();
        assertTrue(jmxReporter instanceof Slf4jReporter);

        final Slf4jReporter reporter = (Slf4jReporter) jmxReporter;
        reporter.close();
    }

    private ObjectName buildObjectName(final UUID clientId, final String beanName) throws MalformedObjectNameException {
        return new ObjectName(String.format(MantaClientAgent.FMT_MBEAN_OBJECT_NAME, beanName, clientId));
    }

}
