/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

import static com.joyent.manta.serialization.ReflectionUtils.getField;
import static com.joyent.manta.serialization.ReflectionUtils.readField;

/**
 * Serialization class that allows for (de)serialization of
 * <code>sun.security.pkcs11.SessionRef</code> instances.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class SessionRefSerializer extends AbstractManualSerializer<Object> {
    /**
     * Class reference to {@link sun.security.pkcs11.SessionRef}.
     */
    @SuppressWarnings("unchecked")
    private static final Class<Object> SESSION_REF_CLASS =
            (Class<Object>)ReflectionUtils.findClass("sun.security.pkcs11.SessionRef");

    private static final Field ID_FIELD = getField(SESSION_REF_CLASS, "id");
    private static final Field TOKEN_FIELD = getField(SESSION_REF_CLASS, "token");
    private static final Field REFERENT_FIELD = getField(SESSION_REF_CLASS, "referent");
    private static final Field QUEUE_FIELD = getField(SESSION_REF_CLASS, "queue");
    private static final Field NEXT_FIELD = getField(SESSION_REF_CLASS, "next");
    private static final Field DISCOVERED_FIELD = getField(SESSION_REF_CLASS, "discovered");

    /**
     * Creates a new serializaer instance.
     */
    public SessionRefSerializer() {
        super(SESSION_REF_CLASS);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(final Kryo kryo, final Output output, final Object object) {
        final long id = (long)ReflectionUtils.readField(ID_FIELD, object);
        output.writeVarLong(id, true);
        kryo.writeClassAndObject(output, readField(TOKEN_FIELD, object));
        kryo.writeClassAndObject(output, readField(REFERENT_FIELD, object));
        kryo.writeClassAndObject(output, readField(QUEUE_FIELD, object));
        kryo.writeClassAndObject(output, readField(NEXT_FIELD, object));
        kryo.writeClassAndObject(output, readField(DISCOVERED_FIELD, object));
    }

    @Override
    public Object read(final Kryo kryo, final Input input, final Class<Object> type) {
        final long id = input.readVarLong(true);
        final Object token = kryo.readClassAndObject(input);
        final Object referent = kryo.readClassAndObject(input);
        final Object queue = kryo.readClassAndObject(input);
        final Object next = kryo.readClassAndObject(input);
        final Object discovered = kryo.readClassAndObject(input);

        try {
            Constructor<?> constructor = SESSION_REF_CLASS.getDeclaredConstructors()[0];
            constructor.setAccessible(true);

            Object instance = constructor.newInstance(referent, id, token);
            ReflectionUtils.writeField(QUEUE_FIELD, instance, queue);
            ReflectionUtils.writeField(NEXT_FIELD, instance, next);
            ReflectionUtils.writeField(DISCOVERED_FIELD, instance, discovered);
            return instance;
        } catch (ReflectiveOperationException e) {
            String msg = "Error instantiating class";
            MantaClientSerializationException mcse = new MantaClientSerializationException(msg, e);
            mcse.setContextValue("instanceClass", SESSION_REF_CLASS.getCanonicalName());
            throw mcse;
        }
    }
}
