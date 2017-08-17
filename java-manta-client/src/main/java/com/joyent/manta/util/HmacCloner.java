/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import com.joyent.manta.exception.MantaReflectionException;
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.macs.HMac;
import org.bouncycastle.util.Memoable;

import java.lang.reflect.Field;

import static org.apache.commons.lang3.reflect.FieldUtils.getField;
import static org.apache.commons.lang3.reflect.FieldUtils.readField;
import static org.apache.commons.lang3.reflect.FieldUtils.writeField;

/**
 * Utility class for cloning HMac objects.
 */
public final class HmacCloner implements Cloner<HMac> {

    /**
     * Private field on {@link HMac} to query for ipad state.
     */
    private static final Field FIELD_IPAD_STATE = getField(HMac.class, "ipadState", true);

    /**
     * Private field on {@link HMac} to query for opad state.
     */
    private static final Field FIELD_OPAD_STATE = getField(HMac.class, "opadState", true);

    @Override
    public HMac createClone(final HMac source) {
        final Digest originalDigest = source.getUnderlyingDigest();
        final Digest clonedDigest = new DigestCloner().createClone(originalDigest);
        final HMac cloned = new HMac(clonedDigest);

        try {
            final Memoable ipadState = (Memoable) readField(FIELD_IPAD_STATE, source, true);
            final Memoable opadState = (Memoable) readField(FIELD_OPAD_STATE, source, true);

            writeField(FIELD_IPAD_STATE, cloned, ipadState.copy());
            writeField(FIELD_OPAD_STATE, cloned, opadState.copy());
        } catch (IllegalAccessException e) {
            throw new MantaReflectionException(e);
        }

        return cloned;
    }
}
