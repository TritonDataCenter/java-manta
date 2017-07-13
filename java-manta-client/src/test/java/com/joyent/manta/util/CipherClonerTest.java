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
import java.io.IOException;
import java.io.OutputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;

public class CipherClonerTest {
    @Test
    public void testCanCloneCipher() throws IOException, InvalidAlgorithmParameterException, InvalidKeyException {
        final SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
        final SecretKey secretKey = SecretKeyUtils.generate(cipherDetails);
        final byte[] iv = cipherDetails.generateIv();
        final byte[] inputData = RandomUtils.nextBytes(cipherDetails.getBlockSizeInBytes() * 3);

        final Cipher originalCipher = cipherDetails.getCipher();
        originalCipher.init(Cipher.ENCRYPT_MODE, secretKey, cipherDetails.getEncryptionParameterSpec(iv));

        // TODO: not a real clone yet
        final Cipher clonedCipher = new CipherCloner().clone(originalCipher);

        final ByteArrayOutputStream originalOutput = new ByteArrayOutputStream();
        final CipherOutputStream originalCipherOutput = new CipherOutputStream(originalOutput, originalCipher);
        originalCipherOutput.write(inputData);
        originalCipherOutput.flush();
        originalCipherOutput.close();

        final ByteArrayOutputStream clonedOutput = new ByteArrayOutputStream();
        final CipherOutputStream clonedCipherOutput = new CipherOutputStream(clonedOutput, originalCipher);
        clonedCipherOutput.write(inputData);
        clonedCipherOutput.flush();
        clonedCipherOutput.close();

        final byte[] originalEncrypted = originalOutput.toByteArray();
        final byte[] clonedEncrypted = clonedOutput.toByteArray();

        Assert.assertEquals(inputData.length, originalEncrypted.length);
        AssertJUnit.assertArrayEquals(originalEncrypted, clonedEncrypted);
    }
}
