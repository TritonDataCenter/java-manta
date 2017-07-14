/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
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
public final class HMacCloner extends AbstractCloner<HMac> {

    /**
     * Private field on {@link HMac} to query for ipad state.
     */
    private static final Field ipadStateField = getField(HMac.class, "ipadState", true);

    /**
     * Private field onf {@link HMac} to query for opad state.
     */
    private static final Field opadStateField = getField(HMac.class, "opadState", true);

    @Override
    public HMac clone(final HMac source) {
        final Digest originalDigest = source.getUnderlyingDigest();
        final Digest clonedDigest = new DigestCloner().clone(originalDigest);
        final HMac cloned = new HMac(clonedDigest);

        overwrite(source, cloned);

        return cloned;
    }

    @Override
    public void overwrite(final HMac source, final HMac target) {
        try {
            final Memoable ipadState = (Memoable) readField(ipadStateField, source, true);
            final Memoable opadState = (Memoable) readField(opadStateField, source, true);

            writeField(ipadStateField, target, ipadState.copy());
            writeField(opadStateField, target, opadState.copy());
        } catch (IllegalAccessException e) {
            throw new MantaReflectionException(e);
        }
    }
}
