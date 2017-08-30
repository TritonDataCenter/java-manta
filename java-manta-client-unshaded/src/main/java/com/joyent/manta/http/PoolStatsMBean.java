/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.pool.PoolStats;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanConstructorInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.ReflectionException;
import java.lang.ref.WeakReference;

/**
 * Class providing real-time information on the connection pool statistics
 * via JMX.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class PoolStatsMBean implements DynamicMBean {
    /**
     * Array of all beans exposed to JMX.
     */
    private static final MBeanAttributeInfo[] M_BEAN_ATTRIBUTES_INFO;

    /**
     * Array of all constructors exposed to JMX.
     */
    private static final MBeanConstructorInfo[] M_BEAN_CONSTRUCTOR_INFO;

    /**
     * Bean info object that encompasses all bean metadata.
     */
    private static final MBeanInfo M_BEAN_INFO;

    /**
     * Total number of properties exposed to JMX from this MBean.
     */
    public static final int NO_OF_PROPERTIES = 4;

    static {
        M_BEAN_ATTRIBUTES_INFO = new MBeanAttributeInfo[] {
                new MBeanAttributeInfo("leased", Integer.class.getName(),
                        "The number of persistent connections",
                        true, false, false),
                new MBeanAttributeInfo("pending", Integer.class.getName(),
                        "The number of connection requests being blocked awaiting a free connection",
                        true, false, false),
                new MBeanAttributeInfo("available", Integer.class.getName(),
                        "The number of idle persistent connections",
                        true, false, false),
                new MBeanAttributeInfo("max", Integer.class.getName(),
                        "The maximum number of allowed persistent connections",
                        true, false, false)
        };

        M_BEAN_CONSTRUCTOR_INFO = new MBeanConstructorInfo[] {
                new MBeanConstructorInfo("PoolingHttpClientConnectionManagerConstructor",
                        "Constructs a new instance based on a connection factory",
                        new MBeanParameterInfo[] {
                            new MBeanParameterInfo("PoolingHttpClientConnectionManager",
                                    PoolingHttpClientConnectionManager.class.getName(),
                                    "PoolingHttpClientConnectionManager instance")
                })
        };

        M_BEAN_INFO = new MBeanInfo(PoolStats.class.getName(),
                "Java Manta SDK HTTP Connection Pool Statistics",
                M_BEAN_ATTRIBUTES_INFO, M_BEAN_CONSTRUCTOR_INFO,
                new MBeanOperationInfo[0], new MBeanNotificationInfo[0]);
    }

    /**
     * Connection pool stats object that is periodically updated by JMX.
     */
    private PoolStats stats;

    /**
     * Weak reference (so we don't block the GC) to a connection manager that
     * provides the {@link PoolStats} object.
     */
    private final WeakReference<PoolingHttpClientConnectionManager> connectionManagerRef;


    /**
     * Creates a new MBean instance backed by the passed connection manager.
     * @param connectionManager instance to get statistics from
     */
    PoolStatsMBean(final PoolingHttpClientConnectionManager connectionManager) {
        this.connectionManagerRef = new WeakReference<>(connectionManager);
        updatePoolStats();
    }

    @Override
    public Object getAttribute(final String attribute) throws AttributeNotFoundException,
            MBeanException, ReflectionException {
        updatePoolStats();
        Integer result = getAttributeFromPoolStats(attribute, this.stats);

        if (result == null) {
            String msg = String.format("Can't find MBean attribute: %s", attribute);
            throw new AttributeNotFoundException(msg);
        }

        return result;
    }

    @Override
    public void setAttribute(final Attribute attribute) throws AttributeNotFoundException,
            InvalidAttributeValueException, MBeanException, ReflectionException {
        throw new AttributeNotFoundException("Not supported");
    }

    @Override
    public AttributeList getAttributes(final String[] attributes) {
        updatePoolStats();

        AttributeList list = new AttributeList(NO_OF_PROPERTIES);

        for (String a : attributes) {
            Integer result = getAttributeFromPoolStats(a, this.stats);
            if (result != null) {
                list.add(result);
            }
        }

        return list;
    }

    @Override
    public AttributeList setAttributes(final AttributeList attributes) {
        return null;
    }

    @Override
    public Object invoke(final String actionName, final Object[] params, final String[] signature)
            throws MBeanException, ReflectionException {
        return null;
    }

    @Override
    public MBeanInfo getMBeanInfo() {
        return M_BEAN_INFO;
    }

    /**
     * Updates the backing {@link PoolStats} object so that JMX data can be
     * refreshed.
     */
    private void updatePoolStats() {
        if (connectionManagerRef.isEnqueued()) {
            return;
        }

        PoolingHttpClientConnectionManager manager = connectionManagerRef.get();

        if (manager != null) {
            this.stats = manager.getTotalStats();
        }

    }

    /**
     * Utility method that pulls an attribute's value from the statistics object.
     * @param attribute attribute to pull
     * @param stats status object to query
     * @return result as integer or null if no mapping exists
     */
    private static Integer getAttributeFromPoolStats(final String attribute,
                                                     final PoolStats stats) {
        switch (attribute) {
            case "leased":
                return stats.getLeased();
            case "pending":
                return stats.getPending();
            case "available":
                return stats.getAvailable();
            case "max":
                return stats.getMax();
            default:
                return null;
        }
    }
}
