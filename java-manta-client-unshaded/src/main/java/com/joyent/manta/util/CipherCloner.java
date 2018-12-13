/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import com.joyent.manta.client.crypto.ExternalSecurityProviderLoader;
import com.joyent.manta.exception.MantaMemoizationException;

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

    static {
        // omit deep-cloning of classes which don't hold cipher state
        // references will be copied directly instead of recursively cloned
        INSTANCE.dontCloneInstanceOf(Class.class, Provider.class);
    }

    @Override
    public Cipher createClone(final Cipher source) {
        /* We are assured that the PKCS11 provider we are getting is libnss and
         * we know that it isn't possible to clone ciphers from this provider. */
        final Provider pkcs11Provider = ExternalSecurityProviderLoader.getPkcs11Provider();
        /* This is the provider of the input cipher and we will not know what
         * provider it is until run time. */
        final Provider cipherProvider = source.getProvider();

        /* We need to validate that the provider as given from the supplied
         * cipher is not backed by a libnss provider because it is unclonable.
         * In that case, we want to emit an exception that leads a developer to
         * a sensible error message rather than the inscrutable message that is
         * returned if we attempt the clone operation. */

        if (pkcs11Provider != null && cipherProvider == pkcs11Provider) {
            String msg = String.format("Cannot create clone of Cipher with provider: %s",
                    source.getProvider());
            throw new MantaMemoizationException(msg);
        }

        return INSTANCE.deepClone(source);
    }
}
