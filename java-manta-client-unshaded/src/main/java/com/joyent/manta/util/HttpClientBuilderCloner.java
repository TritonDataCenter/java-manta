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
 * @author <a href="https://github.com/tjcelaya">Tomas Celayac</a>
 * @since 3.1.7
 */
public final class HttpClientBuilderCloner implements Cloner<HttpClientBuilder> {

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
    private static final List<Field> FIELDS_CLONE_COLLECTION = new ArrayList<>();

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

        final List<String> manuallyCopiedFields = Arrays.asList(
                "requestFirst",
                "requestLast",
                "responseFirst",
                "responseLast");

        for (final Field f : FieldUtils.getAllFields(httpClientBuilderClass)) {
            if (manuallyCopiedFields.contains(f.getName())) {
                FIELDS_CLONE_COLLECTION.add(f);
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

            for (final Field f : FIELDS_CLONE_COLLECTION) {
                final LinkedList sourceList = (LinkedList) readField(f, source, true);
                final LinkedList targetList = new LinkedList();

                targetList.addAll(sourceList);

                writeField(f, target, targetList, true);
            }
        } catch (final ReflectiveOperationException e) {
            throw new MantaReflectionException("Unable to copy HttpClientBuilder field", e);
        }

        return target;
    }
}
