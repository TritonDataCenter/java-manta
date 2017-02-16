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

/**
 * Utility class providing helper methods for use with reflection.
 * We intentionally are not using external dependencies for this
 * functionality because we want to limit our dependency scope
 * because we will need to reshade within this module.
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
     * @throws UnsupportedOperationException if class can't be found
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

        final Class<?>[] classes = new Class[objects.length];

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
            String msg = "Error instantiating [%s] class";
            MantaClientSerializationException mcse = new MantaClientSerializationException(msg, e);
            mcse.setContextValue("instanceClass", instanceClass.getCanonicalName());
            mcse.setContextValue("params", MantaUtils.asString(types));
            throw mcse;
        }
    }
}
