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

/**
 * Utility class for cloning HMac objects.
 */
public final class HMacCloner extends AbstractCloner<HMac> {

    @SuppressWarnings("checkstyle:JavadocMethod")
    private HMacCloner() {
        super(HMac.class);
    }

    /**
     * Create a clone of an HMac object by copying fields.
     *
     * @param original the source HMac to clone
     * @return a new HMac with the same state as the original
     */
    public static HMac clone(final HMac original) {
        final Digest originalDigest = original.getUnderlyingDigest();
        final Digest clonedDigest = DigestCloner.clone(originalDigest);
        final HMac cloned = new HMac(clonedDigest);

        // int digestSize;
        // int blockLength;
        // Memoable ipadState;
        // Memoable opadState;
        // byte[] inputPad;
        // byte[] outputBuf;

        return cloned;
    }
}
