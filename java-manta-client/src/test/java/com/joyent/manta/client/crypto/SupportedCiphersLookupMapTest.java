/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import org.testng.Assert;
import org.testng.annotations.Test;

import javax.crypto.Cipher;

import static com.joyent.manta.client.crypto.SupportedCiphersLookupMap.INSTANCE;

@Test
public class SupportedCiphersLookupMapTest {
    public void canGetAllCiphers() {
        Assert.assertTrue(INSTANCE.get("AES128/GCM/NoPadding") instanceof AesGcmCipherDetails);
        Assert.assertTrue(INSTANCE.get("AES192/GCM/NoPadding") instanceof AesGcmCipherDetails);
        Assert.assertTrue(INSTANCE.get("AES256/GCM/NoPadding") instanceof AesGcmCipherDetails);

        Assert.assertTrue(INSTANCE.get("AES128/CTR/NoPadding") instanceof AesCtrCipherDetails);
        Assert.assertTrue(INSTANCE.get("AES192/CTR/NoPadding") instanceof AesCtrCipherDetails);
        Assert.assertTrue(INSTANCE.get("AES256/CTR/NoPadding") instanceof AesCtrCipherDetails);

        Assert.assertTrue(INSTANCE.get("AES128/CBC/PKCS5Padding") instanceof AesCbcCipherDetails);
        Assert.assertTrue(INSTANCE.get("AES192/CBC/PKCS5Padding") instanceof AesCbcCipherDetails);
        Assert.assertTrue(INSTANCE.get("AES256/CBC/PKCS5Padding") instanceof AesCbcCipherDetails);
    }

    public void canFindCiphers() {
        for (SupportedCipherDetails cipherDetails : INSTANCE.values()) {
            Cipher cipher = SupportedCipherDetails.findCipher(cipherDetails.getCipherAlgorithm(),
                    BouncyCastleLoader.BOUNCY_CASTLE_PROVIDER);
            Assert.assertNotNull(cipher, "Couldn't find cipher for algorithm: " + cipherDetails);

            Cipher cipherLowercase = SupportedCipherDetails.findCipher(cipherDetails.getCipherAlgorithm().toLowerCase(),
                    BouncyCastleLoader.BOUNCY_CASTLE_PROVIDER);
            Assert.assertNotNull(cipherLowercase, "Couldn't find cipher for algorithm: " + cipherDetails);
        }
    }
}
