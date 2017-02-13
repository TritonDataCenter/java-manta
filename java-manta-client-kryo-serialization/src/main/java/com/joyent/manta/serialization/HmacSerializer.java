/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.EncodableDigest;
import org.bouncycastle.crypto.digests.GeneralDigest;
import org.bouncycastle.crypto.macs.HMac;

import java.lang.reflect.Field;

/**
 * Kryo serializer that deconstructs a BouncyCastle {@link HMac} instance
 * and allows for serialization and deserialization.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class HmacSerializer extends Serializer<HMac> {
    /**
     * Private field on {@link HMac} to query for ipad state.
     */
    private final Field ipadStateField = FieldUtils.getField(
            HMac.class, "ipadState", true);
    /**
     * Private field onf {@link HMac} to query for opad state.
     */
    private final Field opadStateField = FieldUtils.getField(
            HMac.class, "opadState", true);

    /**
     * Creates a new Kryo serializer for {@link HMac} objects.
     * @param kryo Kryo instance
     */
    public HmacSerializer(final Kryo kryo) {
        super(false);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(final Kryo kryo, final Output output, final HMac object) {
        try {
            final EncodableDigest ipadState = (EncodableDigest) FieldUtils.readField(
                    ipadStateField, object, true);
            final EncodableDigest opadState = (EncodableDigest) FieldUtils.readField(
                    opadStateField, object, true);

            final EncodableDigest digest = (EncodableDigest) object.getUnderlyingDigest();

            output.writeString(digest.getClass().getCanonicalName());
            output.writeInt(digest.getEncodedState().length);
            output.write(digest.getEncodedState());
            output.writeInt(ipadState.getEncodedState().length);
            output.write(ipadState.getEncodedState());
            output.writeInt(opadState.getEncodedState().length);
            output.write(opadState.getEncodedState());
            output.flush();
        } catch (IllegalAccessException e) {
            String msg = String.format("Error reading private field from [%s] class",
                    object.getClass().getName());
            throw new SerializationException(msg);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public HMac read(final Kryo kryo, final Input input, final Class<HMac> type) {
        String digestClassName = input.readString();
        final Class<GeneralDigest> digestClass;

        try {
            digestClass = (Class<GeneralDigest>)Class.forName(digestClassName);
        } catch (ClassNotFoundException e) {
            throw new SerializationException("No class found with the name: " + digestClassName);
        }

        byte[] digestStateBytes = input.readBytes(input.readInt());
        Digest digest = newInstance(digestClass, digestStateBytes);
        byte[] ipadStateBytes = input.readBytes(input.readInt());
        Digest ipadState = newInstance(digestClass, ipadStateBytes);
        byte[] opadStateBytes = input.readBytes(input.readInt());
        Digest opadState = newInstance(digestClass, opadStateBytes);

        HMac hmac = new HMac(digest);

        try {
            FieldUtils.writeField(ipadStateField, hmac, ipadState, true);
            FieldUtils.writeField(opadStateField, hmac, opadState, true);
        } catch (IllegalAccessException e) {
            String msg = String.format("Error setting private field on [%s] class",
                    HMac.class.getName());
            throw new SerializationException(msg);
        }

        return hmac;
    }

    /**
     * Creates a new {@link Digest} populated with the specified state.
     *
     * @param digestClass digest class to create
     * @param state byte array of inner state
     * @return new digest class instance
     */
    @SuppressWarnings("unchecked")
    private Digest newInstance(final Class<?> digestClass,
                               final byte[] state) {
        try {
            final Object[] params = new Object[] {state};
            return (Digest)ConstructorUtils.invokeConstructor(digestClass, params);
        } catch (ReflectiveOperationException e) {
            String msg = String.format("Error instantiating [%s] class",
                    HMac.class.getName());
            throw new SerializationException(msg);
        }
    }
}
