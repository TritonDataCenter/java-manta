/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Provider;
import java.security.Security;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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
     * System property identifier used to indicate the preferred security provider
     * for client-side encryption.
     */
    private static final String PREFERRED_PROVIDER_SYS_PROP_KEY = "manta.preferred.security.provider";

    /**
     * Name of BouncyCastle security provider.
     */
    private static final String BC_PROVIDER_NAME = "org.bouncycastle.jce.provider.BouncyCastleProvider";

    /**
     * Name of native PKCS11 NSS security provider.
     */
    private static final String PKCS11_PROVIDER_NAME = "SunPKCS11-NSS";

    /**
     * Name of the Sun/Oracle JCE cryptographic provider.
     */
    private static final String SUNJCE_PROVIDER_NAME = "SunJCE";

    /**
     * Reference to the PKCS11 Provider.
     */
    private static final Provider PKCS11_PROVIDER;

    /**
     * Reference to the Bouncy Castle Provider.
     */
    private static final Provider BC_PROVIDER;

    /**
     * Reference to the Sun JCE Provider.
     */
    private static final Provider SUNJCE_PROVIDER;

    /**
     * A unique list of security provider supported for client-side encryption
     * in ranked order of preference.
     */
    private static final List<Provider> RANKED_PREFERRED_PROVIDERS;

    /**
     * Reference to the most preferred cipher provider.
     */
    private static final Provider PREFERRED_PROVIDER;

    static {
        PKCS11_PROVIDER = Security.getProvider(PKCS11_PROVIDER_NAME);

        if (PKCS11_PROVIDER != null) {
            LOGGER.debug("PKCS11 NSS provider was loaded - native crypto support enabled");
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

        SUNJCE_PROVIDER = Security.getProvider(SUNJCE_PROVIDER_NAME);

        RANKED_PREFERRED_PROVIDERS = Collections.unmodifiableList(getPreferredProvidersRanked());

        if (RANKED_PREFERRED_PROVIDERS.isEmpty()) {
            String msg = "There are no usable security providers for Manta "
                    + "client-side encryption";
            throw new SecurityException(msg);
        }

        PREFERRED_PROVIDER = RANKED_PREFERRED_PROVIDERS.get(0);

        LOGGER.info("Java security preferred provider chosen for CSE: {}", PREFERRED_PROVIDER);
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

    public static Provider getSunJceProvider() {
        return SUNJCE_PROVIDER;
    }

    /**
     * Checks to see if a given provider as specified by name is a known and
     * valid security provider.
     *
     * @param provider name/id of security provider
     * @return true if known and valid, otherwise false
     */
    private static boolean isKnownSecurityProvider(final String provider) {
        if (StringUtils.isBlank(provider)) {
            return false;
        }

        switch (provider) {
            case PKCS11_PROVIDER_NAME:
            case BC_PROVIDER_NAME:
            case SUNJCE_PROVIDER_NAME:
                return Security.getProvider(provider) != null;
            default:
                return false;
        }
    }

    /**
     * Chooses the default security provider based on an internal ranking
     * (pkcs11, Bouncy Castle, SunJCE) or the JVM system property
     * <pre>manta.preferred.security.provider</pre>.
     *
     * @return the security provider to use for client side encryption
     */
    public static Provider getPreferredProvider() {
        return PREFERRED_PROVIDER;
    }

    /**
     * @return a list of valid security providers ranked from most preferred to least
     */
    public static List<Provider> getPreferredProvidersRanked() {
        final String preferredProvider = System.getProperty(PREFERRED_PROVIDER_SYS_PROP_KEY);
        final ArrayDeque<Provider> rankedProviders = new ArrayDeque<>(4);

        if (PKCS11_PROVIDER != null) {
            rankedProviders.add(PKCS11_PROVIDER);
        }

        if (BC_PROVIDER != null) {
            rankedProviders.add(BC_PROVIDER);
        }

        if (SUNJCE_PROVIDER != null) {
            rankedProviders.add(SUNJCE_PROVIDER);
        }

        if (isKnownSecurityProvider(preferredProvider)) {
            final Provider preferred = Security.getProvider(preferredProvider);
            rankedProviders.addFirst(preferred);
        }

        return rankedProviders.stream().distinct().collect(Collectors.toList());
    }
}
