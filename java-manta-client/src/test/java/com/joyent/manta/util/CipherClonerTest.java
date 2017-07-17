/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import com.joyent.manta.client.crypto.AesCtrCipherDetails;
import com.joyent.manta.client.crypto.ExternalSecurityProviderLoader;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.exception.MantaEncryptionException;
import org.apache.commons.lang3.RandomUtils;
import org.bouncycastle.jcajce.io.CipherOutputStream;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.SkipException;
import org.testng.annotations.Test;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Provider;

public class CipherClonerTest {
    @Test
    public void testCanCloneCipher()
            throws IOException, InvalidAlgorithmParameterException, InvalidKeyException, BadPaddingException, IllegalBlockSizeException {
        final SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
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
