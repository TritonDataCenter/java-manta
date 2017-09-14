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
import com.joyent.manta.client.crypto.ExternalSecurityProviderLoader;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.exception.MantaMemoizationException;
import org.apache.commons.lang3.RandomUtils;
import org.bouncycastle.jcajce.io.CipherOutputStream;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.SkipException;
import org.testng.annotations.Test;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.io.ByteArrayOutputStream;
import java.security.Provider;

public class CipherClonerTest {

    @Test
    public void testRefusesToClonePKCS11Cipher() {
        final Provider pkcs11Provider = ExternalSecurityProviderLoader.getPkcs11Provider();
        if (pkcs11Provider == null) {
            throw new SkipException("PKCS11 Security Provider not present.");
        }

        // verify that the default Cipher provider is PKCS11 when it is installed
        // the assertThrows below depends on this behavior
        Assert.assertSame(ExternalSecurityProviderLoader.getPreferredProvider(), pkcs11Provider);

        Assert.assertThrows(MantaMemoizationException.class, () -> {
            new CipherCloner().createClone(DefaultsConfigContext.DEFAULT_CIPHER.getCipher());
        });
    }

    @Test
    public void testCanCloneAesGcm128() throws Exception {
        canCloneCipher(AesGcmCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void testCanCloneAesGcm192() throws Exception {
        canCloneCipher(AesGcmCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void testCanCloneAesGcm256() throws Exception {
        canCloneCipher(AesGcmCipherDetails.INSTANCE_256_BIT);
    }

    @Test
    public void testCanCloneAesCtr128() throws Exception {
        canCloneCipher(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void testCanCloneAesCtr192() throws Exception {
        canCloneCipher(AesCtrCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void testCanCloneAesCtr256() throws Exception {
        canCloneCipher(AesCtrCipherDetails.INSTANCE_256_BIT);
    }

    @Test
    public void testCanCloneAesCbc128() throws Exception {
        canCloneCipher(AesCbcCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void testCanCloneAesCbc192() throws Exception {
        canCloneCipher(AesCbcCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void testCanCloneAesCbc256() throws Exception {
        canCloneCipher(AesCbcCipherDetails.INSTANCE_256_BIT);
    }

    private void canCloneCipher(final SupportedCipherDetails cipherDetails) throws Exception {
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
        // we don't want to close originalCipherOutput because that would finalize originalCipher
        // and allow it to be reused below

        final ByteArrayOutputStream clonedOutput = new ByteArrayOutputStream();
        final CipherOutputStream clonedCipherOutput = new CipherOutputStream(clonedOutput, clonedCipher);
        clonedCipherOutput.write(inputData);
        clonedCipherOutput.flush();

        final byte[] originalEncrypted = originalOutput.toByteArray();
        final byte[] clonedEncrypted = clonedOutput.toByteArray();

        Assert.assertEquals(originalEncrypted.length, clonedEncrypted.length);
        AssertJUnit.assertArrayEquals(originalEncrypted, clonedEncrypted);
    }
}
