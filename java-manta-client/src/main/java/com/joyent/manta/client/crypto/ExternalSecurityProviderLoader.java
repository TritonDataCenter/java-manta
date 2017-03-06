/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import org.apache.commons.lang3.ObjectUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Provider;
import java.security.Security;

/**
 * This class statically loads the Legion of the Bouncy Castle encryption
 * libraries into memory, so that they are available to all consumers.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public final class ExternalSecurityProviderLoader {
    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ExternalSecurityProviderLoader.class);

    /**
     * Name of BouncyCastle security provider.
     */
    private static final String BC_PROVIDER_NAME = "org.bouncycastle.jce.provider.BouncyCastleProvider";

    /**
     * Name of native PKCS11 NSS security provider.
     */
    private static final String PKCS11_PROVIDER_NAME = "SunPKCS11-NSS";

    /**
     * Reference to the PKCS11 Provider.
     */
    private static final Provider PKCS11_PROVIDER;

    /**
     * Reference to the Bouncy Castle Provider.
     */
    private static final Provider BC_PROVIDER;

    static {
        final Provider pkcs11Provider = Security.getProvider(PKCS11_PROVIDER_NAME);

        if (pkcs11Provider != null) {
            PKCS11_PROVIDER = pkcs11Provider;
            LOGGER.debug("PKCS11 NSS provider was loaded - native crypto support enabled");
        } else {
            PKCS11_PROVIDER = null;
        }

        final Provider bouncyCastleProvider = Security.getProvider(BC_PROVIDER_NAME);
        // When null the Bouncy Castle provider hasn't been loaded,
        // so we add it as a provider
        if (bouncyCastleProvider == null) {
            BC_PROVIDER = new BouncyCastleProvider();
            Security.addProvider(BC_PROVIDER);
            LOGGER.debug("Bouncy Castle provider was not loaded, adding to providers");
        } else {
            BC_PROVIDER = bouncyCastleProvider;
        }
    }

    /**
     * Private constructor because this class only does static loading.
     */
    private ExternalSecurityProviderLoader() {
    }

    public static Provider getPkcs11Provider() {
        return PKCS11_PROVIDER;
    }

    public static Provider getBouncyCastleProvider() {
        return BC_PROVIDER;
    }

    public static Provider getPreferredProvider() {
        return ObjectUtils.firstNonNull(PKCS11_PROVIDER, BC_PROVIDER);
    }
}
