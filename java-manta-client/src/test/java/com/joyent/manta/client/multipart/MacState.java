/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.bouncycastle.crypto.digests.EncodableDigest;
import org.bouncycastle.crypto.digests.GeneralDigest;
import org.bouncycastle.crypto.macs.HMac;

import java.io.Serializable;
import java.lang.reflect.Field;

/**
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class MacState implements Serializable {
    private static final long serialVersionUID = 2922271139148855128L;

    private Class<?> digestClass;
    private byte[] digestEncodedState;
    private byte[] ipadEncodedState;
    private byte[] opadEncodedState;

    private static final Field IPAD_STATE_FIELD = FieldUtils.getField(
            HMac.class, "ipadState", true);
    private static final Field OPAD_STATE_FIELD = FieldUtils.getField(
            HMac.class, "opadState", true);

    public MacState() {
    }

    public MacState(final HMac hmac) {
        try {
            final EncodableDigest ipadState = (EncodableDigest) FieldUtils.readField(
                    IPAD_STATE_FIELD, hmac, true);
            final EncodableDigest opadState = (EncodableDigest) FieldUtils.readField(
                    OPAD_STATE_FIELD, hmac, true);

            final EncodableDigest digest = (EncodableDigest)hmac.getUnderlyingDigest();

            setDigestClass(digest.getClass());
            setDigestEncodedState(digest.getEncodedState());
            setIpadEncodedState(ipadState.getEncodedState());
            setOpadEncodedState(opadState.getEncodedState());
        } catch (IllegalAccessException e) {
            String msg = String.format("Error reading private field from [%s] class",
                    hmac.getClass().getName());
            throw new SerializationException(msg);
        }
    }

    public Class<?> getDigestClass() {
        return digestClass;
    }

    public MacState setDigestClass(Class<?> digestClass) {
        this.digestClass = digestClass;
        return this;
    }

    public byte[] getDigestEncodedState() {
        return digestEncodedState;
    }

    public MacState setDigestEncodedState(byte[] digestEncodedState) {
        this.digestEncodedState = digestEncodedState;
        return this;
    }

    public byte[] getIpadEncodedState() {
        return ipadEncodedState;
    }

    public MacState setIpadEncodedState(byte[] ipadEncodedState) {
        this.ipadEncodedState = ipadEncodedState;
        return this;
    }

    public byte[] getOpadEncodedState() {
        return opadEncodedState;
    }

    public MacState setOpadEncodedState(byte[] opadEncodedState) {
        this.opadEncodedState = opadEncodedState;
        return this;
    }

    public HMac newInstanceFromState() {
        GeneralDigest digest = newInstance(getDigestClass(),
                getDigestEncodedState());

        GeneralDigest ipadState = newInstance(getDigestClass(),
                getIpadEncodedState());
        GeneralDigest opadState = newInstance(getDigestClass(),
                getOpadEncodedState());

        HMac hmac = new HMac(digest);

        try {
            FieldUtils.writeField(IPAD_STATE_FIELD, hmac, ipadState, true);
            FieldUtils.writeField(OPAD_STATE_FIELD, hmac, opadState, true);
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
            return (GeneralDigest) ConstructorUtils.invokeConstructor(digestClass, state);
        } catch (ReflectiveOperationException e) {
            String msg = String.format("Error instantiating [%s] class",
                    HMac.class.getName());
            throw new SerializationException(msg);
        }
    }
}
