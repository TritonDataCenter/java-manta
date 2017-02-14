/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.serialization;

import com.esotericsoftware.kryo.Serializer;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.macs.HMac;

import javax.crypto.Cipher;
import java.lang.reflect.Field;

/**
 * Abstract class providing reflection helper methods for use with
 * serialization.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 *
 * @param <T> type to serialize
 */
public abstract class AbstractManualSerializer<T> extends Serializer<T> {
    private Class<T> classReference;

    public AbstractManualSerializer(final Class<T> classReference) {
        this.classReference = classReference;
    }

    /**
     * @param acceptsNull
     * @see #setAcceptsNull(boolean)
     */
    public AbstractManualSerializer(final Class<T> classReference,
                                    final boolean acceptsNull) {
        super(acceptsNull);

        this.classReference = classReference;
    }

    /**
     * @param acceptsNull
     * @param immutable
     * @see #setAcceptsNull(boolean)
     * @see #setImmutable(boolean)
     */
    public AbstractManualSerializer(final Class<T> classReference,
                                    final boolean acceptsNull,
                                    final boolean immutable) {
        super(acceptsNull, immutable);
        this.classReference = classReference;
    }

    protected Field captureField(final String fieldName) {
        return FieldUtils.getField(classReference, fieldName, true);
    }

    protected Object readField(final Field field, Object object) {
        try {
            return FieldUtils.readField(field, object, true);
        } catch (IllegalAccessException e) {
            String msg = String.format("Error reading private field from [%s] class",
                    object.getClass().getName());
            throw new SerializationException(msg);
        }
    }

    protected void writeField(final Field field, final Object target, final Object value) {
        try {
            FieldUtils.writeField(field, target, value);
        } catch (IllegalAccessException e) {
            String msg = String.format("Unable to write value [%s] to field [%s]",
                    value, field);
            throw new SerializationException(msg, e);
        }
    }

    protected static Class<?> findClass(final String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            String msg = String.format("Class not found in class path: %s",
                    className);
            throw new UnsupportedOperationException(msg, e);
        }
    }

    /**
     * Creates a new instance with the specified constructor parameters.
     *
     * @param params constructor parameters
     * @return new instance
     */
    protected T newInstance(final Object... params) {
        return newInstance(classReference, params);
    }

    /**
     * Creates a new instance with the specified constructor parameters.
     *
     * @param instanceClass class to instantiate
     * @param params constructor parameters
     * @return new instance
     */
    protected <R> R newInstance(final Class<R> instanceClass,
                              final Object... params) {
        try {
            return ConstructorUtils.invokeConstructor(instanceClass, params);
        } catch (ReflectiveOperationException e) {
            String msg = String.format("Error instantiating [%s] class",
                    HMac.class.getName());
            throw new SerializationException(msg);
        }
    }
}
