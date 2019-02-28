/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.Test;

import java.security.Provider;
import java.util.List;

import static com.joyent.manta.client.crypto.ExternalSecurityProviderLoader.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Test
public class ExternalSecurityProviderLoaderTest {
    static {
        /* We force the class to be loaded here in order to execute the code paths
         * defined in static scope. */
        @SuppressWarnings({"UnusedVariable", "unused"})
        Class<ExternalSecurityProviderLoader> c = ExternalSecurityProviderLoader.class;
    }

    public void canLoadBouncyCastleProvider() {
        assertNotNull(getBouncyCastleProvider(),
                "Bouncy Castle provider wasn't loaded");
    }

    public void canLoadSunJCEProvider() {
        assertNotNull(getSunJceProvider(),
                "SunJCE provider wasn't loaded");
    }

    public void willLoadTheDefaultProvidersRankedWithNullInput() {
        final List<Provider> providers = buildRankedPreferredProviders(null);

        if (getPkcs11Provider() != null) {
            assertProviderOrder(providers, PKCS11_PROVIDER_NAME, BC_PROVIDER_NAME, SUNJCE_PROVIDER_NAME);
        } else {
            assertProviderOrder(providers, BC_PROVIDER_NAME, SUNJCE_PROVIDER_NAME);
        }
    }

    public void willLoadTheDefaultProvidersRankedWithEmptyInput() {
        final List<Provider> providers = buildRankedPreferredProviders("");

        if (getPkcs11Provider() != null) {
            assertProviderOrder(providers, PKCS11_PROVIDER_NAME, BC_PROVIDER_NAME, SUNJCE_PROVIDER_NAME);
        } else {
            assertProviderOrder(providers, BC_PROVIDER_NAME, SUNJCE_PROVIDER_NAME);
        }
    }

    public void willLoadProvidersRankedWithSunJceOthersUnspecified() {
        final String csv = StringUtils.joinWith(",", SUNJCE_PROVIDER_NAME);
        final List<Provider> providers = buildRankedPreferredProviders(csv);

        if (getPkcs11Provider() != null) {
            assertProviderOrder(providers, SUNJCE_PROVIDER_NAME, PKCS11_PROVIDER_NAME, BC_PROVIDER_NAME);
        } else {
            assertProviderOrder(providers, SUNJCE_PROVIDER_NAME, BC_PROVIDER_NAME);
        }
    }

    public void willLoadProvidersRankedWithBcOthersUnspecified() {
        final String csv = StringUtils.joinWith(",", BC_PROVIDER_NAME);
        final List<Provider> providers = buildRankedPreferredProviders(csv);

        if (getPkcs11Provider() != null) {
            assertProviderOrder(providers, BC_PROVIDER_NAME, PKCS11_PROVIDER_NAME, SUNJCE_PROVIDER_NAME);
        } else {
            assertProviderOrder(providers, BC_PROVIDER_NAME, SUNJCE_PROVIDER_NAME);
        }
    }

    public void willLoadProvidersRankedWithSunJceNssBcOrder() {
        final String csv = StringUtils.joinWith(",",
                SUNJCE_PROVIDER_NAME, PKCS11_PROVIDER_NAME, BC_PROVIDER_NAME);
        final List<Provider> providers = buildRankedPreferredProviders(csv);

        if (getPkcs11Provider() != null) {
            assertProviderOrder(providers, SUNJCE_PROVIDER_NAME, PKCS11_PROVIDER_NAME, BC_PROVIDER_NAME);
        } else {
            assertProviderOrder(providers, SUNJCE_PROVIDER_NAME, BC_PROVIDER_NAME);
        }
    }

    public void willLoadProvidersRankedWithSunJceNssBcOrderAndUnknownStripped() {
        final String csv = StringUtils.joinWith(",",
                SUNJCE_PROVIDER_NAME, "Unknown", PKCS11_PROVIDER_NAME,
                BC_PROVIDER_NAME);
        final List<Provider> providers = buildRankedPreferredProviders(csv);

        if (getPkcs11Provider() != null) {
            assertProviderOrder(providers, SUNJCE_PROVIDER_NAME, PKCS11_PROVIDER_NAME, BC_PROVIDER_NAME);
        } else {
            assertProviderOrder(providers, SUNJCE_PROVIDER_NAME, BC_PROVIDER_NAME);
        }
    }

    public void willLoadProvidersRankedWithSunJceNssBcOrderAndSpacesStripped() {
        final String csv = StringUtils.joinWith(",",
                SUNJCE_PROVIDER_NAME + " ", PKCS11_PROVIDER_NAME, " ",
                BC_PROVIDER_NAME);
        final List<Provider> providers = buildRankedPreferredProviders(csv);

        if (getPkcs11Provider() != null) {
            assertProviderOrder(providers, SUNJCE_PROVIDER_NAME, PKCS11_PROVIDER_NAME, BC_PROVIDER_NAME);
        } else {
            assertProviderOrder(providers, SUNJCE_PROVIDER_NAME, BC_PROVIDER_NAME);
        }
    }

    private static void assertProviderOrder(final List<Provider> actualProviders,
                                            final String... expectedProviderNames) {
        try {
            for (int i = 0; i < expectedProviderNames.length; i++) {
                assertEquals(actualProviders.get(i).getName(), expectedProviderNames[i]);
            }
        } catch (RuntimeException e) {
            System.err.println("Actual provider order: " + StringUtils.join(expectedProviderNames));
            throw e;
        }
    }
}
