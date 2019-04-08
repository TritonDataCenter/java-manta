/*
 * Copyright (c) 2017-2019, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import org.testng.Assert;
import org.testng.annotations.Test;

import javax.crypto.Cipher;
import java.security.Provider;
import java.util.List;

import static com.joyent.manta.client.crypto.SupportedCiphersLookupMap.INSTANCE;
import static com.joyent.manta.client.crypto.ExternalSecurityProviderLoader.PKCS11_PROVIDER_NAME;

@SuppressWarnings("BadImport")
@Test
public class SupportedCiphersLookupMapTest {

    public void canGetRestrictedCiphers() {
        Assert.assertTrue(INSTANCE.get("AES128/GCM/NoPadding") instanceof AesGcmCipherDetails);
        Assert.assertTrue(INSTANCE.get("AES128/CTR/NoPadding") instanceof AesCtrCipherDetails);
        Assert.assertTrue(INSTANCE.get("AES128/CBC/PKCS5Padding") instanceof AesCbcCipherDetails);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canGetAllCiphers() {
        Assert.assertTrue(INSTANCE.get("AES192/GCM/NoPadding") instanceof AesGcmCipherDetails);
        Assert.assertTrue(INSTANCE.get("AES256/GCM/NoPadding") instanceof AesGcmCipherDetails);

        Assert.assertTrue(INSTANCE.get("AES192/CTR/NoPadding") instanceof AesCtrCipherDetails);
        Assert.assertTrue(INSTANCE.get("AES256/CTR/NoPadding") instanceof AesCtrCipherDetails);

        Assert.assertTrue(INSTANCE.get("AES192/CBC/PKCS5Padding") instanceof AesCbcCipherDetails);
        Assert.assertTrue(INSTANCE.get("AES256/CBC/PKCS5Padding") instanceof AesCbcCipherDetails);
    }

    /**
     * Test all providers registered at runtime to see if the expected ciphers
     * can be loaded. This provides a basic sanity check against the cryptographic
     * providers that makes sure that we can get at the ciphers that are expected
     * to be present.
     */
    public void canFindCiphers() {
        final List<Provider> providers = ExternalSecurityProviderLoader.getRankedPreferredProviders();

        for (Provider provider : providers) {
            for (SupportedCipherDetails cipherDetails : INSTANCE.values()) {
                /* Libnss doesn't support accessing GCM, so we are not able to
                 * use from that provider. */
                if (provider.getName().equals(PKCS11_PROVIDER_NAME) && cipherDetails instanceof AesGcmCipherDetails) {
                    continue;
                }

                String cipherAlgorithm = cipherDetails.getCipherAlgorithm();
                Cipher cipher = SupportedCipherDetails.findCipher(cipherAlgorithm, provider);
                Assert.assertNotNull(cipher, "Couldn't find cipher for algorithm: " + cipherDetails);

                Cipher cipherLowercase = SupportedCipherDetails.findCipher(cipherAlgorithm.toLowerCase(),
                        provider);
                Assert.assertNotNull(cipherLowercase, "Couldn't find cipher for algorithm: " + cipherDetails);
            }
        }
    }
}
