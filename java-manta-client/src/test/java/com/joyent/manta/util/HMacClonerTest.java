/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import com.joyent.manta.client.crypto.*;
import org.apache.commons.lang3.RandomUtils;
import org.bouncycastle.crypto.macs.HMac;
import org.bouncycastle.crypto.params.KeyParameter;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import javax.crypto.SecretKey;
import java.io.IOException;

import static com.joyent.manta.client.crypto.SupportedHmacsLookupMap.Name.HmacMD5;

@Test
public class HMacClonerTest {
    public void testEncryptionStateCanBeCloned() throws IOException {
        final SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
        final SecretKey key = SecretKeyUtils.generate(cipherDetails);

        final HMac originalHmac = SupportedHmacsLookupMap.INSTANCE.get(HmacMD5.name()).get();
        originalHmac.init(new KeyParameter(key.getEncoded()));
        final HMac clonedHmac = HMacCloner.clone(originalHmac);
        Assert.assertNotNull(clonedHmac);

        final byte[] inputData = RandomUtils.nextBytes(512);

        final byte[] originalComputed = new byte[originalHmac.getMacSize()];
        final byte[] clonedComputed = new byte[originalHmac.getMacSize()];

        originalHmac.update(inputData, 0, inputData.length);
        originalHmac.doFinal(originalComputed, 0);

        clonedHmac.update(inputData, 0, inputData.length);
        clonedHmac.doFinal(clonedComputed, 0);


        AssertJUnit.assertArrayEquals(originalComputed, clonedComputed);
    }
}