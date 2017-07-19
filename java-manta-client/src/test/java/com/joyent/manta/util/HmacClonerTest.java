/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import com.joyent.manta.client.crypto.AesCbcCipherDetails;
import com.joyent.manta.client.crypto.AesCtrCipherDetails;
import com.joyent.manta.client.crypto.AesGcmCipherDetails;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.crypto.SupportedHmacsLookupMap;
import org.apache.commons.lang3.RandomUtils;
import org.bouncycastle.crypto.macs.HMac;
import org.bouncycastle.crypto.params.KeyParameter;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import javax.crypto.SecretKey;
import java.io.IOException;

public class HmacClonerTest {

    private static final String[] HMAC_NAMES = new String[] {
            "HmacMD5",
            "HmacSHA1",
            "HmacSHA256",
            "HmacSHA384",
            "HmacSHA512",
    };

    @Test
    public void testHMacStateCanBeClonedAfterInitializeAesCtr128AllHMacs() throws IOException {
        for (String hmacName : HMAC_NAMES) {
            testHMacStateCanBeClonedAfterInitialization(AesCtrCipherDetails.INSTANCE_128_BIT, hmacName);
        }
    }

    @Test(groups = {"unlimited-crypto"})
    public void testHMacStateCanBeClonedAfterInitializeAesCtr192AllHMacs() throws IOException {
        for (String hmacName : HMAC_NAMES) {
            testHMacStateCanBeClonedAfterInitialization(AesCtrCipherDetails.INSTANCE_192_BIT, hmacName);
        }
    }

    @Test(groups = {"unlimited-crypto"})
    public void testHMacStateCanBeClonedAfterInitializeAesCtr256AllHMacs() throws IOException {
        for (String hmacName : HMAC_NAMES) {
            testHMacStateCanBeClonedAfterInitialization(AesCtrCipherDetails.INSTANCE_256_BIT, hmacName);
        }
    }

    @Test
    public void testHMacStateCanBeClonedAfterInitializeAesGcm128AllHMacs() throws IOException {
        for (String hmacName : HMAC_NAMES) {
            testHMacStateCanBeClonedAfterInitialization(AesGcmCipherDetails.INSTANCE_128_BIT, hmacName);
        }
    }

    @Test(groups = {"unlimited-crypto"})
    public void testHMacStateCanBeClonedAfterInitializeAesGcm192AllHMacs() throws IOException {
        for (String hmacName : HMAC_NAMES) {
            testHMacStateCanBeClonedAfterInitialization(AesGcmCipherDetails.INSTANCE_192_BIT, hmacName);
        }
    }

    @Test(groups = {"unlimited-crypto"})
    public void testHMacStateCanBeClonedAfterInitializeAesGcm256AllHMacs() throws IOException {
        for (String hmacName : HMAC_NAMES) {
            testHMacStateCanBeClonedAfterInitialization(AesGcmCipherDetails.INSTANCE_256_BIT, hmacName);
        }
    }

    @Test
    public void testHMacStateCanBeClonedAfterInitializeAesCbc128AllHMacs() throws IOException {
        for (String hmacName : HMAC_NAMES) {
            testHMacStateCanBeClonedAfterInitialization(AesCbcCipherDetails.INSTANCE_128_BIT, hmacName);
        }
    }

    @Test(groups = {"unlimited-crypto"})
    public void testHMacStateCanBeClonedAfterInitializeAesCbc192AllHMacs() throws IOException {
        for (String hmacName : HMAC_NAMES) {
            testHMacStateCanBeClonedAfterInitialization(AesCbcCipherDetails.INSTANCE_192_BIT, hmacName);
        }
    }

    @Test(groups = {"unlimited-crypto"})
    public void testHMacStateCanBeClonedAfterInitializeAesCbc256AllHMacs() throws IOException {
        for (String hmacName : HMAC_NAMES) {
            testHMacStateCanBeClonedAfterInitialization(AesCbcCipherDetails.INSTANCE_256_BIT, hmacName);
        }
    }

    @Test
    public void testHMacStateCanBeClonedAfterUseAesCtr128AllHMacs() throws IOException {
        for (String hmacName : HMAC_NAMES) {
            testHMacStateCanBeClonedAfterUse(AesCtrCipherDetails.INSTANCE_128_BIT, hmacName);
        }
    }

    @Test(groups = {"unlimited-crypto"})
    public void testHMacStateCanBeClonedAfterUseAesCtr192AllHMacs() throws IOException {
        for (String hmacName : HMAC_NAMES) {
            testHMacStateCanBeClonedAfterInitialization(AesCtrCipherDetails.INSTANCE_192_BIT, hmacName);
        }
    }

    @Test(groups = {"unlimited-crypto"})
    public void testHMacStateCanBeClonedAfterUseAesCtr256AllHMacs() throws IOException {
        for (String hmacName : HMAC_NAMES) {
            testHMacStateCanBeClonedAfterInitialization(AesCtrCipherDetails.INSTANCE_256_BIT, hmacName);
        }
    }

    @Test
    public void testHMacStateCanBeClonedAfterUseAesGcm128AllHMacs() throws IOException {
        for (String hmacName : HMAC_NAMES) {
            testHMacStateCanBeClonedAfterUse(AesGcmCipherDetails.INSTANCE_128_BIT, hmacName);
        }
    }

    @Test(groups = {"unlimited-crypto"})
    public void testHMacStateCanBeClonedAfterUseAesGcm192AllHMacs() throws IOException {
        for (String hmacName : HMAC_NAMES) {
            testHMacStateCanBeClonedAfterInitialization(AesGcmCipherDetails.INSTANCE_192_BIT, hmacName);
        }
    }

    @Test(groups = {"unlimited-crypto"})
    public void testHMacStateCanBeClonedAfterUseAesGcm256AllHMacs() throws IOException {
        for (String hmacName : HMAC_NAMES) {
            testHMacStateCanBeClonedAfterInitialization(AesGcmCipherDetails.INSTANCE_256_BIT, hmacName);
        }
    }

    @Test
    public void testHMacStateCanBeClonedAfterUseAesCbc128AllHMacs() throws IOException {
        for (String hmacName : HMAC_NAMES) {
            testHMacStateCanBeClonedAfterUse(AesCbcCipherDetails.INSTANCE_128_BIT, hmacName);
        }
    }

    @Test(groups = {"unlimited-crypto"})
    public void testHMacStateCanBeClonedAfterUseAesCbc192AllHMacs() throws IOException {
        for (String hmacName : HMAC_NAMES) {
            testHMacStateCanBeClonedAfterInitialization(AesCbcCipherDetails.INSTANCE_192_BIT, hmacName);
        }
    }

    @Test(groups = {"unlimited-crypto"})
    public void testHMacStateCanBeClonedAfterUseAesCbc256AllHMacs() throws IOException {
        for (String hmacName : HMAC_NAMES) {
            testHMacStateCanBeClonedAfterInitialization(AesCbcCipherDetails.INSTANCE_256_BIT, hmacName);
        }
    }

    private void testHMacStateCanBeClonedAfterInitialization(SupportedCipherDetails cipherDetails, final String hmacName) {
        final SecretKey key = SecretKeyUtils.generate(cipherDetails);

        final HMac originalHmac = SupportedHmacsLookupMap.INSTANCE.get(hmacName).get();
        originalHmac.init(new KeyParameter(key.getEncoded()));
        final HMac clonedHmac = new HmacCloner().createClone(originalHmac);

        final byte[] inputData = RandomUtils.nextBytes(cipherDetails.getBlockSizeInBytes() * 3);
        originalHmac.update(inputData, 0, inputData.length);
        clonedHmac.update(inputData, 0, inputData.length);

        final byte[] originalComputed = new byte[originalHmac.getMacSize()];
        final byte[] clonedComputed = new byte[originalHmac.getMacSize()];
        originalHmac.doFinal(originalComputed, 0);
        clonedHmac.doFinal(clonedComputed, 0);

        AssertJUnit.assertArrayEquals(originalComputed, clonedComputed);
    }

    private void testHMacStateCanBeClonedAfterUse(final SupportedCipherDetails cipherDetails, final String hmacName) {
        final SecretKey key = SecretKeyUtils.generate(cipherDetails);

        final HMac originalHmac = SupportedHmacsLookupMap.INSTANCE.get(hmacName).get();
        originalHmac.init(new KeyParameter(key.getEncoded()));

        final byte[] firstUpdate = RandomUtils.nextBytes(cipherDetails.getBlockSizeInBytes() * 3);
        originalHmac.update(firstUpdate, 0, firstUpdate.length);
        final HMac clonedHmac = new HmacCloner().createClone(originalHmac);

        final byte[] inputData = RandomUtils.nextBytes(cipherDetails.getBlockSizeInBytes() * 3);
        originalHmac.update(inputData, 0, inputData.length);
        clonedHmac.update(inputData, 0, inputData.length);

        final byte[] originalComputed = new byte[originalHmac.getMacSize()];
        final byte[] clonedComputed = new byte[originalHmac.getMacSize()];

        originalHmac.doFinal(originalComputed, 0);
        clonedHmac.doFinal(clonedComputed, 0);

        AssertJUnit.assertArrayEquals(originalComputed, clonedComputed);
    }
}