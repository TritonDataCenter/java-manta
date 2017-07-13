/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.macs.HMac;
import org.bouncycastle.util.Memoable;

import java.lang.reflect.Field;

import static com.joyent.manta.util.MantaReflectionUtils.readField;
import static com.joyent.manta.util.MantaReflectionUtils.writeField;

/**
 * Utility class for cloning HMac objects.
 */
public class HMacCloner extends AbstractCloner<HMac> {

    /**
     * Private field on {@link HMac} to query for ipad state.
     */
    private final Field ipadStateField = captureField("ipadState");

    /**
     * Private field onf {@link HMac} to query for opad state.
     */
    private final Field opadStateField = captureField("opadState");

    /**
     * Construct a HMacCloner.
     */
    HMacCloner() {
        super(HMac.class);
    }

    /**
     * Create a clone of an HMac object by copying fields.
     *
     * @param original the source HMac to clone
     * @return a new HMac with the same state as the original
     */
    public HMac clone(final HMac original) {
        final Digest originalDigest = original.getUnderlyingDigest();
        final Digest clonedDigest = DigestCloner.clone(originalDigest);

        final Memoable ipadState = (Memoable) readField(ipadStateField, original);
        final Memoable opadState = (Memoable) readField(opadStateField, original);

        final HMac cloned = new HMac(clonedDigest);
        writeField(ipadStateField, cloned, ipadState.copy());
        writeField(opadStateField, cloned, opadState.copy());

        return cloned;
    }
}
