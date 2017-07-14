/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.serialization;

import com.esotericsoftware.kryo.Serializer;

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
    /**
     * Class to be serialized.
     */
    private Class<T> classReference;

    /**
     * Creates a new serializer instance for the specified class.
     *
     * @param classReference class to be serialized
     */
    public AbstractManualSerializer(final Class<T> classReference) {
        super(false, true);
        this.classReference = classReference;
    }

    /**
     * Creates a new serializer instance for the specified class.
     *
     * @param classReference class to be serialized
     * @param acceptsNull if true, this serializer will handle null values
     * @see #setAcceptsNull(boolean)
     */
    public AbstractManualSerializer(final Class<T> classReference,
                                    final boolean acceptsNull) {
        super(acceptsNull, true);

        this.classReference = classReference;
    }

    /**
     * * Creates a new serializer instance for the specified class.
     *
     * @param classReference class to be serialized
     * @param acceptsNull if true, this serializer will handle null values
     * @param immutable if true, copy() will return the original object
     * @see #setAcceptsNull(boolean)
     * @see #setImmutable(boolean)
     */
    public AbstractManualSerializer(final Class<T> classReference,
                                    final boolean acceptsNull,
                                    final boolean immutable) {
        super(acceptsNull, immutable);
        this.classReference = classReference;
    }

    /**
     * Gets a reference to a field on an object.
     *
     * @param fieldName field to get
     * @return reference to an object's field
     * @throws UnsupportedOperationException when the field isn't present
     */
    protected Field captureField(final String fieldName) {
        final Field field = ReflectionUtils.getField(classReference, fieldName);

        if (field == null) {
            String msg = String.format("No field [%s] found on object [%s]",
                    classReference, fieldName);
            throw new UnsupportedOperationException(msg);
        }

        return field;
    }

    /**
     * Creates a new instance with the specified constructor parameters.
     *
     * @param params constructor parameters
     * @return new instance
     */
    protected T newInstance(final Object... params) {
        return ReflectionUtils.newInstance(classReference, params);
    }
}
