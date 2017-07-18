/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import com.joyent.manta.client.crypto.AesCtrCipherDetails;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import org.apache.commons.lang3.RandomUtils;
import org.bouncycastle.jcajce.io.CipherOutputStream;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.io.ByteArrayOutputStream;

public class CipherClonerTest {

    @Test
    public void testCanCloneAesCtr128() throws Exception {
        canCloneCipher(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    private void canCloneCipher(final SupportedCipherDetails cipherDetails) throws Exception {
        // TODO: paramaterize cipherDetails

        final SecretKey secretKey = SecretKeyUtils.generate(cipherDetails);
        final byte[] iv = cipherDetails.generateIv();
        final byte[] inputData = RandomUtils.nextBytes(cipherDetails.getBlockSizeInBytes() * 3);

        // notice we are specifically calling getBouncyCastleCipher()
        final Cipher originalCipher = cipherDetails.getBouncyCastleCipher();
        originalCipher.init(Cipher.ENCRYPT_MODE, secretKey, cipherDetails.getEncryptionParameterSpec(iv));

        final Cipher clonedCipher = new CipherCloner().createClone(originalCipher);

        final ByteArrayOutputStream originalOutput = new ByteArrayOutputStream();
        final CipherOutputStream originalCipherOutput = new CipherOutputStream(originalOutput, originalCipher);
        originalCipherOutput.write(inputData);
        originalCipherOutput.flush();
        // close calls doFinal, but we want to leave originalCipher in an intermediate state
        // originalCipherOutput.close();

        final ByteArrayOutputStream clonedOutput = new ByteArrayOutputStream();
        final CipherOutputStream clonedCipherOutput = new CipherOutputStream(clonedOutput, clonedCipher);
        clonedCipherOutput.write(inputData);
        clonedCipherOutput.flush();
        clonedCipherOutput.close();

        final byte[] originalEncrypted = originalOutput.toByteArray();
        final byte[] clonedEncrypted = clonedOutput.toByteArray();

        Assert.assertEquals(inputData.length, originalEncrypted.length);
        AssertJUnit.assertArrayEquals(originalEncrypted, clonedEncrypted);
    }
}
