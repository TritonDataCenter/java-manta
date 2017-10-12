/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import com.joyent.manta.exception.MantaReflectionException;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.http.impl.client.HttpClientBuilder;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.apache.commons.lang3.reflect.FieldUtils.readField;
import static org.apache.commons.lang3.reflect.FieldUtils.writeField;

/**
 * Object which can be used to clone an {@link HttpClientBuilder} for reuse.
 * This is only really necessary when {@link com.joyent.manta.http.MantaConnectionFactoryConfigurator}
 * is used in combination with {@link com.joyent.manta.client.LazyMantaClient}.
 *
 * Care is taken to create separate instances of mutable lists in {@link HttpClientBuilder} but otherwise
 * deep cloning is avoided.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celayac</a>
 * @since 3.1.7
 */
@SuppressWarnings("LiteralClassName")
public final class HttpClientBuilderShallowCloner implements ShallowCloner<HttpClientBuilder> {

    /**
     * Class name when caller depends on plain artifact.
     */
    private static final String UNSHADED_CLASS_NAME = "org.apache.http.impl.client.HttpClientBuilder";

    /**
     * Class name when caller depends on bundled artifact.
     */
    private static final String SHADED_CLASS_NAME = "com.joyent.manta.org." + UNSHADED_CLASS_NAME;

    /**
     * Fields to copy directly.
     */
    private static final List<Field> FIELDS_CLONE_DIRECT = new ArrayList<>();

    /**
     * Fields which should be "deep" cloned at just a single level.
     */
    private static final List<Field> FIELDS_CLONE_LIST = new ArrayList<>();

    static {
        Class<?> httpClientBuilderClass = null;
        try {
            httpClientBuilderClass = Class.forName(SHADED_CLASS_NAME);
        } catch (final ClassNotFoundException e) {
            // check the unshaded class name
        }

        if (httpClientBuilderClass == null) {
            try {
                httpClientBuilderClass = Class.forName("org.apache.http.impl.client.HttpClientBuilder");
            } catch (final ClassNotFoundException e) {
                throw new NoClassDefFoundError(
                        String.format(
                                "Unable to find HttpClientBuilder class, checked: %s, %s",
                                SHADED_CLASS_NAME,
                                UNSHADED_CLASS_NAME));
            }
        }

        final List<String> listFields = Arrays.asList(
                "requestFirst",
                "requestLast",
                "responseFirst",
                "responseLast");

        for (final Field f : FieldUtils.getAllFields(httpClientBuilderClass)) {
            if (listFields.contains(f.getName())) {
                FIELDS_CLONE_LIST.add(f);
            } else {
                FIELDS_CLONE_DIRECT.add(f);
            }
        }
    }

    /**
     * Generates a new {@code T} using values from {@code source}.
     *
     * @param source the source of state to use
     * @return a brand new {@code T}
     */
    @Override
    public HttpClientBuilder createClone(final HttpClientBuilder source) {
        final HttpClientBuilder target = HttpClientBuilder.create();

        try {
            for (final Field f : FIELDS_CLONE_DIRECT) {
                writeField(f, target, readField(f, source, true), true);
            }

            for (final Field f : FIELDS_CLONE_LIST) {
                final Object sourceList = readField(f, source, true);

                if (sourceList != null && !(sourceList instanceof LinkedList<?>)) {
                    throw new MantaReflectionException(
                            "Unexpected collection when cloning HttpClientBuilder: "
                                    + sourceList.getClass().getSimpleName());
                }

                if (sourceList == null) {
                    continue;
                }


                writeField(f, target, cloneList((LinkedList<?>) sourceList), true);
            }
        } catch (final ReflectiveOperationException e) {
            throw new MantaReflectionException("Unable to copy HttpClientBuilder field", e);
        }

        return target;
    }

    /**
     * Type-generic list copy.
     *
     * @param sourceList list of elements to copy into the result
     * @param <T> type of list elements
     * @return a new list with the elements from sourceList
     */
    private static <T> LinkedList<T> cloneList(final LinkedList<T> sourceList) {
        final LinkedList<T> targetCollection = new LinkedList<>();
        targetCollection.addAll(sourceList);
        return targetCollection;
    }
}
