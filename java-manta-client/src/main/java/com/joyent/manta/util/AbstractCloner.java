/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import java.lang.reflect.Field;

import static org.apache.commons.lang3.reflect.FieldUtils.readField;
import static org.apache.commons.lang3.reflect.FieldUtils.writeField;

/**
 * Abstract class providing reflection helper methods for use with
 * cloning.
 *
 * @param <T> type to clone
 */
public abstract class AbstractCloner<T> {

    /**
     * Generates a new {@code T} using values from {@code original}
     * @param source the source of state to use
     * @return a brand new {@code T}
     */
    public abstract T clone(final T source);

    /**
     * Overwrites the state of {@code target} with values from {@code source}.
     *
     * @param source the object from which to copy values
     * @param target the object update with state from {@code source}
     * @return
     */
    public abstract void overwrite(final T source, final T target);

    /**
     * Clones a single field from {@code source} to {@code target}.
     *
     * @param source the object from which to copy the value
     * @param target the object update from {@code source}
     * @param field the field to copy
     * @throws IllegalAccessException
     */
    protected void cloneField(final Field field, final T source, final T target) throws IllegalAccessException {
        writeField(field, target, readField(field, source, true));
    }
}
