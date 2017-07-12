/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import com.twmacinta.util.FastMD5Digest;
import org.apache.commons.lang3.NotImplementedException;
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.SHA1Digest;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.digests.SHA384Digest;
import org.bouncycastle.crypto.digests.SHA512Digest;

/**
 * Utility class for cloning Digest objects.
 */
public final class DigestCloner extends AbstractCloner<Digest> {

    @SuppressWarnings("checkstyle:JavadocMethod")
    private DigestCloner() {
        super(Digest.class);
    }

    @SuppressWarnings("checkstyle:JavadocMethod")
    private static FastMD5Digest clone(final FastMD5Digest original) {
        return new FastMD5Digest(original.getEncodedState());
    }

    /**
     * @param original the source Digest to clone
     *
     * @return a new Digest with the same state as the original
     */
    public static Digest clone(final Digest original) {
        if (original instanceof FastMD5Digest) {
            return (Digest) ((FastMD5Digest) original).copy();
        } else if (original instanceof SHA1Digest) {
            return (Digest) ((SHA1Digest) original).copy();
        } else if (original instanceof SHA256Digest) {
            return (Digest) ((SHA256Digest) original).copy();
        } else if (original instanceof SHA384Digest) {
            return (Digest) ((SHA384Digest) original).copy();
        } else if (original instanceof SHA512Digest) {
            return (Digest) ((SHA512Digest) original).copy();
        }

        throw new NotImplementedException("Clone not implemented for type: " + original.getClass().getCanonicalName());
    }
}
