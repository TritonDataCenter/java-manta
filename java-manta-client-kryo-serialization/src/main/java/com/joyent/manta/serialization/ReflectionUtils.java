/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.serialization;

import com.joyent.manta.util.MantaUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Objects;

/**
 * Utility class providing helper methods for use with reflection.
 * We intentionally are not using external dependencies for this
 * functionality because we want to limit our dependency scope
 * because we will need to (re)shade within this module.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
final class ReflectionUtils {
    /**
     * Private constructor for utility class.
     */
    private ReflectionUtils() {
    }

    /**
     * Gets a {@link Class} instance by looking up the name.
     *
     * @param className class name to look up
     * @return class instance
     * @throws MantaClientSerializationException if class can't be found
     */
    static Class<?> findClass(final String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            String msg = "Class not found in class path";
            MantaClientSerializationException mcse = new MantaClientSerializationException(msg, e);
            mcse.setContextValue("className", className);

            throw mcse;
        }
    }

    /**
     * Gets a {@link Class} instance by looking up the name or returns
     * null if not found.
     *
     * @param className class name to look up
     * @return class instance or null if not found
     * @throws MantaClientSerializationException if class can't be found
     */
    static Class<?> findClassOrNull(final String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    /**
     * Gets a {@link Field} reference from a class regardless of scope.
     *
     * @param clazz class to read field from
     * @param name name of field
     * @return reference to field or null if not found
     */
    static Field getField(final Class<?> clazz, final String name) {
        Objects.requireNonNull(clazz, "Class must not be null");
        Objects.requireNonNull(name, "Field name must not be null");

        for (Class<?> acls = clazz; acls != null; acls = acls.getSuperclass()) {
            try {
                final Field field = acls.getDeclaredField(name);

                if (!Modifier.isPublic(field.getModifiers())) {
                    field.setAccessible(true);
                }
                return field;
            } catch (final NoSuchFieldException ex) {
                // just continue the loop if the field can't be found
            }
        }

        Field field = null;

        for (final Class<?> c : clazz.getInterfaces()) {
            try {
                final Field potentialField = c.getField(name);
                if (field != null) {
                    String msg = String.format("Field name [%s] on class [%s] matches "
                        + "field name on two or more interfaces", name, clazz);
                    throw new IllegalStateException(msg);
                }

                field = potentialField;
            } catch (final NoSuchFieldException ex) {
                // just continue the loop if the field can't be found
            }
        }
        return field;
    }

    /**
     * Reads a field from an object.
     *
     * @param field field to read
     * @param object object to read from
     * @return field's value
     */
    static Object readField(final Field field, final Object object) {
        Objects.requireNonNull(field, "Field must not be null");
        Objects.requireNonNull(object, "Object must not be null");

        if (!field.isAccessible()) {
            field.setAccessible(true);
        }

        try {
            return field.get(object);
        } catch (IllegalAccessException e) {
            String msg = String.format("Error reading private field from [%s] class",
                    object.getClass().getName());
            MantaClientSerializationException mcse = new MantaClientSerializationException(msg);
            mcse.setContextValue("field", field.getName());
            mcse.setContextValue("objectClass", object.getClass());
            throw mcse;
        }
    }

    /**
     * Writes a object to an object's field.
     *
     * @param field field to write to
     * @param target target object
     * @param value object to write
     */
    static void writeField(final Field field, final Object target, final Object value) {
        Objects.requireNonNull(field, "Field must not be null");
        Objects.requireNonNull(target, "Target must not be null");

        if (!field.isAccessible()) {
            field.setAccessible(true);
        }

        try {
            field.set(target, value);
        } catch (IllegalAccessException e) {
            String msg = String.format("Unable to write value [%s] to field [%s]",
                    value, field);
            MantaClientSerializationException mcse = new MantaClientSerializationException(msg);
            mcse.setContextValue("field", field.getName());
            mcse.setContextValue("targetClass", target.getClass());
            if (value == null) {
                mcse.setContextValue("valueClass", "null");
            } else {
                mcse.setContextValue("valueClass", value.getClass());
            }
            throw mcse;
        }
    }

    /**
     * Reads the class for an each entry in an array of object and builds a
     * new array with the corresponding class for each object. If an entry
     * in the source array is null, then the corresponding entry in the
     * resulting array is null.
     *
     * @param objects an array of objects to read their class information from
     * @return an array of classes corresponding to each object or null on null input
     */
    static Class<?>[] classesForObjects(final Object... objects) {
        if (objects == null) {
            return null;
        }

        if (objects.length == 0) {
            return new Class<?>[0];
        }

        final Class<?>[] classes = new Class<?>[objects.length];

        for (int i = 0; i < objects.length; i++) {
            if (objects[i] != null) {
                classes[i] = objects[i].getClass();
            }
        }

        return classes;
    }

    /**
     * Creates a new instance with the specified constructor parameters.
     *
     * @param instanceClass class to instantiate
     * @param params constructor parameters
     * @param <R> type of class to instantiate
     * @return new instance
     */
    static <R> R newInstance(final Class<R> instanceClass,
                             final Object... params) {
        final Object[] actualParams;

        if (params == null) {
            actualParams = new Object[0];
        } else {
            actualParams = params;
        }

        final Class<?>[] types = classesForObjects(actualParams);

        try {
            final Constructor<R> constructor = instanceClass.getDeclaredConstructor(types);
            constructor.setAccessible(true);
            return constructor.newInstance(actualParams);
        } catch (ReflectiveOperationException e) {
            String msg = "Error instantiating class";
            MantaClientSerializationException mcse = new MantaClientSerializationException(msg, e);
            mcse.setContextValue("instanceClass", instanceClass.getCanonicalName());
            mcse.setContextValue("params", MantaUtils.asString(types));
            throw mcse;
        }
    }
}
