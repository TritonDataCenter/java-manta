/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Provider;
import java.security.Security;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
     * for client-side encryption. Note: This property is not in
     * {@link com.joyent.manta.config.MapConfigContext} because the system
     * property is read within the static context and is not reread when a new
     * {@link com.joyent.manta.client.MantaClient} is created.
     */
    private static final String PREFERRED_PROVIDERS_SYS_PROP_KEY = "manta.preferred.security.providers";

    /**
     * Name of BouncyCastle security provider.
     */
    static final String BC_PROVIDER_NAME = BouncyCastleProvider.PROVIDER_NAME;

    /**
     * Name of native PKCS11 NSS security provider.
     */
    static final String PKCS11_PROVIDER_NAME = "SunPKCS11-NSS";

    /**
     * Name of the Sun/Oracle JCE cryptographic provider.
     */
    static final String SUNJCE_PROVIDER_NAME = "SunJCE";

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
    private static List<Provider> rankedPreferredProviders;

    static {
        PKCS11_PROVIDER = Security.getProvider(PKCS11_PROVIDER_NAME);

        if (PKCS11_PROVIDER != null) {
            LOGGER.debug("PKCS11 NSS provider was loaded - native crypto support available");
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

        rankedPreferredProviders = Collections.unmodifiableList(
                buildRankedPreferredProviders(System.getProperty(PREFERRED_PROVIDERS_SYS_PROP_KEY)));

        if (rankedPreferredProviders.isEmpty()) {
            String msg = "There are no usable security providers for Manta "
                    + "client-side encryption";
            throw new SecurityException(msg);
        }

        LOGGER.info("Security provider chosen for CSE: {} ", getPreferredProvider());
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
            LOGGER.warn("Blank security provider specified: [{}]", provider);
            return false;
        }

        switch (provider) {
            case PKCS11_PROVIDER_NAME:
            case BC_PROVIDER_NAME:
            case SUNJCE_PROVIDER_NAME:
                final Provider foundProvider = Security.getProvider(provider);

                if (foundProvider == null && LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Known provider specified, but it "
                            + "couldn't be loaded. Provider: [{}]", provider);
                }

                return foundProvider != null;
            default:
                LOGGER.warn("Unknown provider specified: [{}]", provider);
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
        return rankedPreferredProviders.get(0);
    }

    public static List<Provider> getRankedPreferredProviders() {
        return rankedPreferredProviders;
    }

    public static void setRankedPreferredProviders(final Collection<Provider> providersRanked) {
        rankedPreferredProviders = Collections.unmodifiableList(
                new ArrayList<>(providersRanked));
    }

    /**
     * @param preferredProvidersCSV CSV list of security provider names
     * @return a list of valid security providers ranked from most preferred to least
     */
    public static List<Provider> buildRankedPreferredProviders(final String preferredProvidersCSV) {
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

        final String[] providers;

        if (StringUtils.isNotBlank(preferredProvidersCSV)) {
            providers = StringUtils.split(preferredProvidersCSV, ",");
        } else {
            providers = new String[0];
        }

        ArrayUtils.reverse(providers);
        Arrays.stream(providers)
                .map(StringUtils::trimToEmpty)
                .filter(StringUtils::isNotBlank)
                .filter(ExternalSecurityProviderLoader::isKnownSecurityProvider)
                .distinct()
                .forEach(p -> {
                    final Provider preferred = Security.getProvider(p);
                    rankedProviders.addFirst(preferred);
                });

        return rankedProviders.stream().distinct().collect(Collectors.toList());
    }
}
