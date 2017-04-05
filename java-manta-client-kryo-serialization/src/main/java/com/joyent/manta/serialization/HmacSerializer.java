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
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.twmacinta.util.FastMD5Digest;
import com.twmacinta.util.MD5;
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.EncodableDigest;
import org.bouncycastle.crypto.digests.GeneralDigest;
import org.bouncycastle.crypto.digests.MD5Digest;
import org.bouncycastle.crypto.digests.SHA1Digest;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.digests.SHA384Digest;
import org.bouncycastle.crypto.digests.SHA512Digest;
import org.bouncycastle.crypto.macs.HMac;

import java.lang.reflect.Field;

import static com.joyent.manta.serialization.ReflectionUtils.readField;
import static com.joyent.manta.serialization.ReflectionUtils.writeField;

/**
 * Kryo serializer that deconstructs a BouncyCastle {@link HMac} instance
 * and allows for serialization and deserialization.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class HmacSerializer extends AbstractManualSerializer<HMac> {
    /**
     * Private field on {@link HMac} to query for ipad state.
     */
    private final Field ipadStateField = captureField("ipadState");

    /**
     * Private field onf {@link HMac} to query for opad state.
     */
    private final Field opadStateField = captureField("opadState");

    /**
     * Creates a new Kryo serializer for {@link HMac} objects.
     * @param kryo Kryo serializer instance
     */
    public HmacSerializer(final Kryo kryo) {
        super(HMac.class, false);
        registerClasses(kryo);
    }

    /**
     * Registers the classes needed for serialization with Kryo.
     *
     * @param kryo Kryo instance
     */
    private void registerClasses(final Kryo kryo) {
        kryo.register(HMac.class);
        kryo.register(FastMD5Digest.class);
        kryo.register(MD5.class);
        Class<?> md5StateClass = ReflectionUtils.findClass("com.twmacinta.util.MD5State");
        kryo.register(md5StateClass, new CompatibleFieldSerializer<>(kryo, md5StateClass));
        kryo.register(MD5Digest.class);
        kryo.register(SHA1Digest.class);
        kryo.register(SHA256Digest.class);
        kryo.register(SHA384Digest.class);
        kryo.register(SHA512Digest.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(final Kryo kryo, final Output output, final HMac object) {
        final EncodableDigest ipadState = (EncodableDigest)readField(ipadStateField, object);
        final EncodableDigest opadState = (EncodableDigest)readField(opadStateField, object);

        final EncodableDigest digest = (EncodableDigest) object.getUnderlyingDigest();

        kryo.writeObject(output, digest.getClass());
        output.writeInt(digest.getEncodedState().length);
        output.write(digest.getEncodedState());
        output.writeInt(ipadState.getEncodedState().length);
        output.write(ipadState.getEncodedState());
        output.writeInt(opadState.getEncodedState().length);
        output.write(opadState.getEncodedState());

        output.flush();
    }

    @Override
    @SuppressWarnings("unchecked")
    public HMac read(final Kryo kryo, final Input input, final Class<HMac> type) {
        final Class<GeneralDigest> digestClass = (Class<GeneralDigest>)kryo.readObject(input, Class.class);

        byte[] digestStateBytes = input.readBytes(input.readInt());
        Digest digest = ReflectionUtils.newInstance(digestClass, new Object[] {digestStateBytes});
        byte[] ipadStateBytes = input.readBytes(input.readInt());
        Digest ipadState = ReflectionUtils.newInstance(digestClass, new Object[] {ipadStateBytes});
        byte[] opadStateBytes = input.readBytes(input.readInt());
        Digest opadState = ReflectionUtils.newInstance(digestClass, new Object[] {opadStateBytes});

        HMac hmac = new HMac(digest);

        writeField(ipadStateField, hmac, ipadState);
        writeField(opadStateField, hmac, opadState);

        return hmac;
    }

//    @Override
//    @SuppressWarnings("unchecked")
//    public void write(final Kryo kryo, final Output output, final HMac object) {
//        final Digest digest = object.getUnderlyingDigest();
//        kryo.writeClassAndObject(output, digest);
//
//        final EncodableDigest ipadState = (EncodableDigest)readField(ipadStateField, object);
//        final EncodableDigest opadState = (EncodableDigest)readField(opadStateField, object);
//
//        output.writeInt(ipadState.getEncodedState().length);
//        output.write(ipadState.getEncodedState());
//        output.writeInt(opadState.getEncodedState().length);
//        output.write(opadState.getEncodedState());
//
//        output.flush();
//    }
//
//    @Override
//    @SuppressWarnings("unchecked")
//    public HMac read(final Kryo kryo, final Input input, final Class<HMac> type) {
//        final Digest digest = (Digest)kryo.readClassAndObject(input);
//
//        byte[] ipadStateBytes = input.readBytes(input.readInt());
//        Digest ipadState = ReflectionUtils.newInstance(digest.getClass(), new Object[] {ipadStateBytes});
//        byte[] opadStateBytes = input.readBytes(input.readInt());
//        Digest opadState = ReflectionUtils.newInstance(digest.getClass(), new Object[] {opadStateBytes});
//
//        HMac hmac = new HMac(digest);
//
//        writeField(ipadStateField, hmac, ipadState);
//        writeField(opadStateField, hmac, opadState);
//
//        return hmac;
//    }
}
