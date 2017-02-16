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
     */
    public HmacSerializer() {
        super(HMac.class, false);
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
}
