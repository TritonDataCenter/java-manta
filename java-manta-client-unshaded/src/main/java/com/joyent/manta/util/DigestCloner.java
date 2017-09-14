/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import com.joyent.manta.exception.MantaMemoizationException;
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.util.Memoable;

/**
 * Utility class for cloning Digest objects.
 */
public final class DigestCloner implements Cloner<Digest> {

    @Override
    public Digest createClone(final Digest source) {
        if (source instanceof Memoable) {
            return (Digest) ((Memoable) source).copy();
        }

        // we expect all Digest objects to implement Memoable
        throw new MantaMemoizationException("Clone not implemented for type: " + source.getClass().getCanonicalName());
    }
}
