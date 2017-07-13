/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import javax.crypto.Cipher;

public class CipherCloner extends AbstractCloner<Cipher> {

    /**
     * Creates a new serializer instance for the specified class.
     */
    CipherCloner() {
        super(Cipher.class);
    }

    public Cipher clone(Cipher original) {
        return original;
    }
}
