/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.Arrays;
import java.util.Objects;

/**
 * Class providing real-time information on the connection pool statistics
 * via JMX.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class ConfigContextMBean implements DynamicMBean {
    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigContextMBean.class);

    /**
     * Weak reference to configuration context providing configuration info.
     */
    private final WeakReference<ConfigContext> configRef;

    /**
     * Array of all beans exposed to JMX.
     */
    private final MBeanAttributeInfo[] beanAttributeInfo;

    /**
     * Bean info object that encompasses all bean metadata.
     */
    private final MBeanInfo beanInfo;

    /**
     * Flag indicating if the backing context object is mutable.
     */
    private final boolean isSettable;

    /**
     * Creates a new instance of the bean based on the passed context object.
     * @param config backing configuration context to store as weak reference
     */
    public ConfigContextMBean(final ConfigContext config) {
        this.configRef = new WeakReference<>(config);
        this.isSettable = config instanceof SettableConfigContext;
        this.beanAttributeInfo = beanAttributeInfoBuilder();
        this.beanInfo = beanInfoBuilder(this.beanAttributeInfo);
    }

    /**
     * @return an array of all of the attributes to expose to JMX
     */
    private MBeanAttributeInfo[] beanAttributeInfoBuilder() {
            return new MBeanAttributeInfo[] {
                new MBeanAttributeInfo(MapConfigContext.MANTA_URL_KEY,
                        String.class.getName(),
                        "The Manta server URL to connect to",
                        true, this.isSettable, false),
                new MBeanAttributeInfo(MapConfigContext.MANTA_USER_KEY,
                        String.class.getName(),
                        "User connecting to Manta",
                        true, this.isSettable, false),
                new MBeanAttributeInfo(MapConfigContext.MANTA_KEY_ID_KEY,
                        String.class.getName(),
                        "Fingerprint of RSA key used to generate HTTP signatures",
                        true, this.isSettable, false),
                new MBeanAttributeInfo(MapConfigContext.MANTA_KEY_PATH_KEY,
                        String.class.getName(),
                        "Path on the file system to private key used to generate HTTP signatures",
                        true, this.isSettable, false),
                new MBeanAttributeInfo(MapConfigContext.MANTA_TIMEOUT_KEY,
                        Integer.class.getName(),
                        "Connection timeout for the Manta service in milliseconds",
                        true, this.isSettable, false),
                new MBeanAttributeInfo(MapConfigContext.MANTA_RETRIES_KEY,
                        Integer.class.getName(),
                        "Number of times to retry failed requests",
                        true, this.isSettable, false),
                new MBeanAttributeInfo(MapConfigContext.MANTA_MAX_CONNS_KEY,
                        Integer.class.getName(),
                        "Maximum number of open connections to the Manta API",
                        true, this.isSettable, false),
                new MBeanAttributeInfo(MapConfigContext.MANTA_HTTPS_PROTOCOLS_KEY,
                        String.class.getName(),
                        "Comma delimited list of HTTPS protocols",
                        true, this.isSettable, false),
                new MBeanAttributeInfo(MapConfigContext.MANTA_NO_NATIVE_SIGS_KEY,
                        Boolean.class.getName(),
                        "Flag that disables using native code to generate HTTP signatures",
                        true, false, false),
                new MBeanAttributeInfo(MapConfigContext.MANTA_HTTP_BUFFER_SIZE_KEY,
                        Integer.class.getName(),
                        "Size of buffer in bytes to use to buffer streams of HTTP data",
                        true, this.isSettable, false),
                new MBeanAttributeInfo(MapConfigContext.MANTA_TCP_SOCKET_TIMEOUT_KEY,
                        Integer.class.getName(),
                        "Time in milliseconds to wait to see if a TCP socket has timed out",
                        true, this.isSettable, false),
                new MBeanAttributeInfo(MapConfigContext.MANTA_VERIFY_UPLOADS_KEY,
                        Boolean.class.getName(),
                        "Flag indicating the checksum verification of uploaded files is enabled",
                        true, this.isSettable, false),
                new MBeanAttributeInfo(MapConfigContext.MANTA_UPLOAD_BUFFER_SIZE_KEY,
                        Integer.class.getName(),
                        "The size of pre-streaming upload buffers",
                        true, this.isSettable, false),
                new MBeanAttributeInfo(MapConfigContext.MANTA_CLIENT_ENCRYPTION_ENABLED_KEY,
                        Boolean.class.getName(),
                        "Flag indicating client-side encryption is enabled",
                        true, false, false),
                new MBeanAttributeInfo(MapConfigContext.MANTA_ENCRYPTION_KEY_ID_KEY,
                        String.class.getName(),
                        "The unique identifier of the key used for encryption",
                        true, false, false),
                new MBeanAttributeInfo(MapConfigContext.MANTA_ENCRYPTION_ALGORITHM_KEY,
                        String.class.getName(),
                        "The client-side encryption algorithm",
                        true, false, false),
                new MBeanAttributeInfo(MapConfigContext.MANTA_PERMIT_UNENCRYPTED_DOWNLOADS_KEY,
                        Boolean.class.getName(),
                        "Flag indicating that downloading unencrypted files is allowed in encryption mode",
                        true, false, false),
                new MBeanAttributeInfo(MapConfigContext.MANTA_ENCRYPTION_AUTHENTICATION_MODE_KEY,
                        EncryptionAuthenticationMode.class.getName(),
                        "Ciphertext authentication mode",
                        true, false, false),
                new MBeanAttributeInfo(MapConfigContext.MANTA_ENCRYPTION_PRIVATE_KEY_PATH_KEY,
                        String.class.getName(),
                        "Path to the private encryption key on the filesystem",
                        true, false, false)
        };
    }

    /**
     * Builds the metadata object that exposes all attribute and constructor info
     * to JMX.
     *
     * @param attributeInfo array of attributes to populate metadata object with
     * @return fully build metadata object
     */
    private MBeanInfo beanInfoBuilder(final MBeanAttributeInfo[] attributeInfo) {
        MBeanConstructorInfo[] constructorInfo = new MBeanConstructorInfo[] {
                new MBeanConstructorInfo("ConfigContextConstructor",
                        "Constructs a new instance based on a config context",
                        new MBeanParameterInfo[] {
                                new MBeanParameterInfo("ConfigContext",
                                        ConfigContext.class.getName(),
                                        "ConfigContext instance")
                        })
        };

        return new MBeanInfo(ConfigContext.class.getName(),
                "Java Manta SDK Manta Client Configuration",
                attributeInfo, constructorInfo,
                new MBeanOperationInfo[0], new MBeanNotificationInfo[0]);
    }

    @Override
    public Object getAttribute(final String attribute)
            throws AttributeNotFoundException, MBeanException, ReflectionException {
        ConfigContext config = this.configRef.get();

        if (config == null) {
            throw new AttributeNotFoundException("Configuration context has been garbage collected");
        }

        Object result = ConfigContext.getAttributeFromContext(attribute, config);

        if (result == null) {
            String msg = String.format("Can't find MBean attribute: %s", attribute);
            throw new AttributeNotFoundException(msg);
        }

        return result;
    }

    @Override
    public void setAttribute(final Attribute attribute) throws AttributeNotFoundException,
            InvalidAttributeValueException, MBeanException, ReflectionException {
        if (!this.isSettable) {
            throw new UnsupportedOperationException(
                    "Can't set attributes for non-settable context");
        }

        String name = attribute.getName();
        Object value = attribute.getValue();
        boolean keyExists = Arrays.binarySearch(MapConfigContext.ALL_PROPERTIES, name) >= 0;

        if (!keyExists) {
            String msg = String.format("No attribute with the name [%s] found",
                    name);
            throw new AttributeNotFoundException(msg);
        }

        ConfigContext config = this.configRef.get();

        if (config == null) {
            throw new AttributeNotFoundException("Configuration context has been garbage collected");
        }

        SettableConfigContext<?> settable = (SettableConfigContext<?>)config;
        SettableConfigContext.setAttributeFromContext(name, value, settable);
    }

    @Override
    public AttributeList getAttributes(final String[] attributes) {
        ConfigContext config = this.configRef.get();

        if (config == null) {
            throw new IllegalStateException("Configuration context has been garbage collected");
        }

        AttributeList list = new AttributeList(beanAttributeInfo.length);

        for (String a : attributes) {
            Object result = ConfigContext.getAttributeFromContext(a, config);

            if (result instanceof EncryptionAuthenticationMode) {
                result = Objects.toString(result);
            }

            Attribute attribute = new Attribute(a, result);
            list.add(attribute);
        }

        return list;
    }

    @Override
    public AttributeList setAttributes(final AttributeList attributes) {
        if (!this.isSettable) {
            throw new UnsupportedOperationException(
                    "Can't set attributes for non-settable context");
        }

        AttributeList setList = new AttributeList(beanAttributeInfo.length);

        for (Attribute a : attributes.asList()) {
            try {
                setAttribute(a);
                setList.add(a);
            } catch (Exception e) {
                String msg = String.format("Can't modify attribute %s",
                        a.getName());
                LOGGER.warn(msg, e);
            }
        }

        return setList;
    }

    @Override
    public Object invoke(final String actionName, final Object[] params,
                         final String[] signature)
            throws MBeanException, ReflectionException {
        return null;
    }

    @Override
    public MBeanInfo getMBeanInfo() {
        return this.beanInfo;
    }
}
