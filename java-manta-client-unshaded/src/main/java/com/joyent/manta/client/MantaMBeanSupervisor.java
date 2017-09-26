/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.DynamicMBean;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Helper class for keeping track of MBeans associated with a {@link MantaClient}.
 */
public class MantaMBeanSupervisor implements AutoCloseable {

    @SuppressWarnings("JavaDocVariable")
    private static final Logger LOGGER = LoggerFactory.getLogger(MantaMBeanSupervisor.class);

    /**
     * Format string for creating {@link ObjectName}s.
     */
    private static final String FMT_MBEAN_OBJECT_NAME = "com.joyent.manta.client:type=%s[%d]";

    /**
     * A running count of the times we have created new {@link MantaMBeanSupervisor}
     * instances.
     */
    private static final AtomicInteger SUPERVISOR_COUNT = new AtomicInteger(0);

    /**
     * List of all MBeans to be added to JMX.
     */
    private final Map<ObjectName, DynamicMBean> beans;

    /**
     * Supervisor index. Used to avoid JMX {@link ObjectName} collisions.
     */
    private final int idx;

    /**
     * Create a new supervisor that will own an index and a set of beans.
     */
    MantaMBeanSupervisor() {
        idx = SUPERVISOR_COUNT.incrementAndGet();
        beans = new WeakHashMap<>();
    }

    /**
     * Attempts to create an {@link ObjectName} for a {@link DynamicMBean} and register it with the platform's
     * {@link MBeanServer}.
     *
     * @param bean the bean to attempt to register
     */
    public void expose(final DynamicMBean bean) {
        final ObjectName name;
        try {
            name = new ObjectName(String.format(FMT_MBEAN_OBJECT_NAME, bean.getClass().getSimpleName(), idx));
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
     * Deregisters the beans stored in {@code this.beans} so
     * that they are no longer visible via JMX.
     */
    @Override
    public void close() throws Exception {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        for (final Map.Entry<ObjectName, DynamicMBean> bean : beans.entrySet()) {
            try {
                server.unregisterMBean(bean.getKey());
            } catch (final JMException e) {
                String msg = String.format("Error deregistering [%s] MBean in JMX",
                        bean.getKey());
                LOGGER.warn(msg, e);
            }
        }
    }
}
