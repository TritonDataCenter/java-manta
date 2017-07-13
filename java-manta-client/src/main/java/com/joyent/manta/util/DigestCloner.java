/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import org.apache.commons.lang3.NotImplementedException;
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.util.Memoable;

/**
 * Utility class for cloning Digest objects.
 */
final class DigestCloner extends AbstractCloner<Digest> {

    /**
     * Construct a DigestCloner.
     */
    DigestCloner() {
        super(Digest.class);
    }

    /**
     * @param original the source Digest to clone
     *
     * @return a new Digest with the same state as the original
     */
    public Digest clone(final Digest original) {
        if (original instanceof Memoable) {
            return (Digest) ((Memoable) original).copy();
        }

        throw new NotImplementedException("Clone not implemented for type: " + original.getClass().getCanonicalName());
    }
}
