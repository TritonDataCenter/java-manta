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
public final class DigestCloner extends AbstractCloner<Digest> {

    @Override
    public Digest clone(final Digest source) {
        if (source instanceof Memoable) {
            return (Digest) ((Memoable) source).copy();
        }

        throw new NotImplementedException("Clone not implemented for type: " + source.getClass().getCanonicalName());
    }

    public void overwrite(final Digest source, final Digest target) {
        if (source instanceof Memoable && target instanceof Memoable) {
            ((Memoable) target).reset(((Memoable) source).copy());
        }
    }
}
