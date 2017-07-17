/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import com.joyent.manta.client.crypto.ExternalSecurityProviderLoader;
import com.joyent.manta.exception.MantaEncryptionException;

import javax.crypto.Cipher;
import java.security.Provider;

/**
 * Utility class for cloning Cipher objects.
 */
public final class CipherCloner implements Cloner<Cipher> {

    /**
     * Instance of object-cloning library that does deep generic cloning.
     */
    private static final com.rits.cloning.Cloner INSTANCE = new com.rits.cloning.Cloner();

    // TODO: configure Cloner so it knows about immutable properties

    @Override
    public Cipher createClone(final Cipher source) {
        final Provider pkcs11Provider = ExternalSecurityProviderLoader.getPkcs11Provider();
        if (null != pkcs11Provider && pkcs11Provider.equals(source.getProvider())) {
            throw new MantaEncryptionException("Cannot create clone of PKCS11-backed Cipher.");
        }

        return INSTANCE.deepClone(source);
    }
}
