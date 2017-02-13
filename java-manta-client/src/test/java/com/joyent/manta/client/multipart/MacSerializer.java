/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.BeanSerializer;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.bouncycastle.crypto.digests.EncodableDigest;
import org.bouncycastle.crypto.digests.GeneralDigest;
import org.bouncycastle.crypto.macs.HMac;

import java.lang.reflect.Field;

/**
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class MacSerializer extends Serializer<HMac> {
    private final BeanSerializer<MacState> macStateSerializer;

    private final Field ipadStateField = FieldUtils.getField(
            HMac.class, "ipadState", true);
    private final Field opadStateField = FieldUtils.getField(
            HMac.class, "opadState", true);

    public MacSerializer(final Kryo kryo) {
        super(false);
        this.macStateSerializer = new BeanSerializer<>(kryo, MacState.class);
        kryo.register(MacState.class, this.macStateSerializer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(Kryo kryo, Output output, HMac object) {
        final MacState state;
        try {
            final EncodableDigest ipadState = (EncodableDigest) FieldUtils.readField(
                    ipadStateField, object, true);
            final EncodableDigest opadState = (EncodableDigest) FieldUtils.readField(
                    opadStateField, object, true);

            final EncodableDigest digest = (EncodableDigest) object.getUnderlyingDigest();

            state = new MacState()
                    .setDigestClass(digest.getClass())
                    .setDigestEncodedState(digest.getEncodedState())
                    .setIpadEncodedState(ipadState.getEncodedState())
                    .setOpadEncodedState(opadState.getEncodedState());
        } catch (IllegalAccessException e) {
            String msg = String.format("Error reading private field from [%s] class",
                    object.getClass().getName());
            throw new SerializationException(msg);
        }

        macStateSerializer.write(kryo, output, state);
    }

    @Override
    public HMac read(final Kryo kryo, final Input input, final Class<HMac> type) {
        MacState state = macStateSerializer.read(kryo, input, MacState.class);

        GeneralDigest digest = newInstance(state.getDigestClass(),
                state.getDigestEncodedState());

        GeneralDigest ipadState = newInstance(state.getDigestClass(),
                state.getIpadEncodedState());
        GeneralDigest opadState = newInstance(state.getDigestClass(),
                state.getOpadEncodedState());

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

    @SuppressWarnings("unchecked")
    private GeneralDigest newInstance(final Class<?> digestClass,
                                      final byte[] state) {
        try {
            return (GeneralDigest)ConstructorUtils.invokeConstructor(digestClass, state);
        } catch (ReflectiveOperationException e) {
            String msg = String.format("Error instantiating [%s] class",
                    HMac.class.getName());
            throw new SerializationException(msg);
        }
    }
}
