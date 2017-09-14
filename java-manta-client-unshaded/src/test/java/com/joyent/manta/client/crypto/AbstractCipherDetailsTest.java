/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import org.testng.Assert;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Arrays;

public abstract class AbstractCipherDetailsTest {
    protected void sizeCalculationWorksRoundTrip(final SupportedCipherDetails cipherDetails,
                                                 final long plaintextSize) {
        long ciphertextSize = cipherDetails.ciphertextSize(plaintextSize);
        long calculatedPlaintextSize = cipherDetails.plaintextSize(ciphertextSize);

        if (cipherDetails.plaintextSizeCalculationIsAnEstimate()) {
            long difference = calculatedPlaintextSize - plaintextSize;
            Assert.assertTrue(difference < cipherDetails.getBlockSizeInBytes(),
                    "Plaintext size difference was greater than the block size");
        } else {
            Assert.assertEquals(calculatedPlaintextSize, plaintextSize,
                    "Size calculation didn't work for cipher: " + cipherDetails.getCipherId());
        }
    }

    protected void sizeCalculationWorksComparedToActualCipher(final SupportedCipherDetails cipherDetails) throws Exception {
        int arbitrarySize = 30475;
        byte[] plaintext = new byte[arbitrarySize];
        Arrays.fill(plaintext, (byte)0);

        Cipher cipher = cipherDetails.getCipher();
        byte[] iv = new byte[16];
        AlgorithmParameterSpec spec = cipherDetails.getEncryptionParameterSpec(iv);
        SecretKey key = SecretKeyUtils.generate(cipherDetails);
        cipher.init(Cipher.ENCRYPT_MODE, key, spec);

        byte[] ciphertext = cipher.doFinal(plaintext);

        final long ciphertextSize;

        if (cipherDetails.isAEADCipher()) {
            ciphertextSize = ciphertext.length;
        } else {
            ciphertextSize = ciphertext.length + cipherDetails.getAuthenticationTagOrHmacLengthInBytes();
        }

        long calculatedCiphertextSize = cipherDetails.ciphertextSize(plaintext.length);

        Assert.assertEquals(ciphertextSize, calculatedCiphertextSize,
                "Calculated ciphertext size didn't match actual ciphertext size " +
                        "for cipher: " + cipherDetails.getCipherId());

        long calculatedPlaintextSize = cipherDetails.plaintextSize(ciphertextSize);

        if (cipherDetails.plaintextSizeCalculationIsAnEstimate()) {
            long difference = calculatedPlaintextSize - plaintext.length;
            Assert.assertTrue(difference < cipherDetails.getBlockSizeInBytes(),
                    "Plaintext size difference was greater than the block size");
        } else {
            Assert.assertEquals(plaintext.length, calculatedPlaintextSize,
                    "Calculated plaintext size didn't match actual plaintext size " +
                            "for cipher: " + cipherDetails.getCipherId());
        }
    }
}
