/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.management.DynamicMBean;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Helper class for keeping track of MBeans associated with a {@link MantaClient}.
 *
 * Note: The purpose of SUPERVISOR_COUNT is to avoid {@link ObjectName} collisions. Every instance of
 * this class will have a unique index associated with it that will be used to register the MBeans it receives.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 * @since 3.1.7
 */
class MantaClientAgent implements AutoCloseable {

    @SuppressWarnings("JavaDocVariable")
    private static final Logger LOGGER = LoggerFactory.getLogger(MantaClientAgent.class);

    /**
     * Format string for creating {@link ObjectName}s.
     */
    private static final String FMT_MBEAN_OBJECT_NAME = "com.joyent.manta.client:type=%s[%d]";

    /**
     * Supervisor index. Used to avoid JMX {@link ObjectName} collisions.
     */
    private final UUID id;

    /**
     * List of all MBeans to be added to JMX.
     */
    private final Map<ObjectName, DynamicMBean> beans;

    /**
     * Exposes metrics we don't manage as our own MBeans. The relevant type
     * is actually {@link Reporter} but that isn't as interesting as the fact that
     * all reporters are also {@link Closeable}.
     */
    private final Closeable metricReporter;

    /**
     * Flag indicating if the supervisor has been "closed" (i.e. beans deregistered)
     */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    MantaClientAgent() {
        this(UUID.randomUUID(), null);
    }

    /**
     * Create a new supervisor that will own an index and a set of beans.
     */
    MantaClientAgent(final UUID clientId, final MetricRegistry metricRegistry) {
        this.id = clientId;
        this.beans = new HashMap<>(2);

        if (metricRegistry != null) {
            this.metricReporter = JmxReporter.forRegistry(metricRegistry)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build();
        } else {
            this.metricReporter = null;
        }
    }

    /**
     * Attempts to create an {@link ObjectName} for a {@link DynamicMBean} and register it with the platform's
     * {@link MBeanServer}.
     *
     * @param beanable the bean to attempt to register
     */
    void register(final MantaMBeanable beanable) {
        if (closed.get()) {
            throw new IllegalStateException("Cannot register MBeans, supervisor has been closed");
        }

        final DynamicMBean bean = beanable.toMBean();

        if (bean == null) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("MantaMBeanable object returned null");
            }

            return;
        }

        final ObjectName name;
        try {
            name = new ObjectName(String.format(FMT_MBEAN_OBJECT_NAME, bean.getClass().getSimpleName(), id));
        } catch (final JMException e) {
            LOGGER.warn("Error creating bean: " + bean.getClass().getSimpleName(), e);
            return;
        }

        try {
            ManagementFactory.getPlatformMBeanServer().registerMBean(bean, name);
            beans.put(name, bean);
        } catch (final JMException e) {
            LOGGER.warn(String.format("Error registering [%s] MBean in JMX", name), e);
        }
    }

    /**
     * Prepare the supervisor for reuse.
     *
     * @throws Exception if an exception is thrown by {@link #close()}
     */
    void reset() throws Exception {
        if (!beans.isEmpty()) {
            close();
        }

        closed.set(false);
    }

    Map<ObjectName, DynamicMBean> getBeans() {
        return Collections.unmodifiableMap(beans);
    }

    /**
     * Deregisters the beans stored in {@code this.beans} so
     * that they are no longer visible via JMX.
     */
    @Override
    public void close() throws Exception {
        if (closed.get()) {
            return;
        }

        closed.set(true);

        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        for (final Map.Entry<ObjectName, DynamicMBean> bean : beans.entrySet()) {
            try {
                server.unregisterMBean(bean.getKey());
            } catch (final JMException e) {
                LOGGER.warn(String.format("Error deregistering [%s] MBean in JMX", bean.getKey()), e);
            }
        }

        if (metricReporter != null) {
            metricReporter.close();
        }

        beans.clear();
    }

}
