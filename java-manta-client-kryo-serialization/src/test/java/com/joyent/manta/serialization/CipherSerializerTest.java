/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.FastInput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.joyent.manta.client.crypto.*;
import org.testng.AssertJUnit;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

/**
 * Unit tests that verify we can serialize and deserialize a {@link Cipher}
 * object using the Kryo framework.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
@Test
public class CipherSerializerTest {
    private static final byte[] SECRET_KEY_BYTES =
            Base64.getDecoder().decode("qAnCNUmmFjUTtImNGv241Q==");

    private Kryo kryo = new Kryo();

    @BeforeClass
    public void setup() throws Exception {
        kryo.register(Cipher.class, new CipherSerializer(kryo));
    }

    public void canSerializeAesGcm128() throws Exception {
        canSerializeCipher(AesGcmCipherDetails.INSTANCE_128_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSerializeAesGcm192() throws Exception {
        canSerializeCipher(AesGcmCipherDetails.INSTANCE_192_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSerializeAesGcm256() throws Exception {
        canSerializeCipher(AesGcmCipherDetails.INSTANCE_256_BIT, false);
    }

    public void canSerializeAesCtr128() throws Exception {
        canSerializeCipher(AesCtrCipherDetails.INSTANCE_128_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSerializeAesCtr192() throws Exception {
        canSerializeCipher(AesCtrCipherDetails.INSTANCE_192_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSerializeAesCtr256() throws Exception {
        canSerializeCipher(AesCtrCipherDetails.INSTANCE_256_BIT, false);
    }

    public void canSerializeAesCbc128() throws Exception {
        canSerializeCipher(AesCbcCipherDetails.INSTANCE_128_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSerializeAesCbc192() throws Exception {
        canSerializeCipher(AesCbcCipherDetails.INSTANCE_192_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSerializeAesCbc256() throws Exception {
        canSerializeCipher(AesCbcCipherDetails.INSTANCE_256_BIT, false);
    }

    /*
     *  Tests for verifying deserialized ciphers actually have independent state.
     *  TODO: move to it's own test class
     */
    @Test(groups = {"pkcs11"})
    public void canSerializeWithDetachedCloneCtr128() throws Exception {
        if (null == ExternalSecurityProviderLoader.getPkcs11Provider()) {
            throw new SkipException("PKCS11 Security Provider not present, " +
                    "skipping Cipher cloning verification.");
        }

        canSerializeCipher(AesCtrCipherDetails.INSTANCE_128_BIT, true);
    }

    private void canSerializeCipher(final SupportedCipherDetails cipherDetails,
                                    final boolean verifyStateSeparation)
            throws Exception {
        byte[] iv = new byte[cipherDetails.getIVLengthInBytes()];
        Arrays.fill(iv, (byte) 0);
        final SecretKey secretKey = loadSecretKey(cipherDetails);

        byte[][] plainTextChunks = new byte[][] {
                "Text string one two three four five six seven".getBytes(StandardCharsets.UTF_8),
                "eight nine ten eleven twelve thirteen fourteen".getBytes(StandardCharsets.UTF_8)
        };

        final byte[] expected;

        {
            Cipher cipherExpected = cipherDetails.getCipher();
            cipherExpected.init(Cipher.ENCRYPT_MODE, secretKey,
                    cipherDetails.getEncryptionParameterSpec(iv));
            byte[] chunk1 = cipherExpected.update(plainTextChunks[0]);
            byte[] chunk2 = cipherExpected.doFinal(plainTextChunks[1]);

            expected = combineArray(chunk1, chunk2);
        }

        Cipher cipher;

        if (verifyStateSeparation) {
            cipher = cipherDetails.getBouncyCastleCipher();
        } else {
            cipher = cipherDetails.getCipher();
        }


        cipher.init(Cipher.ENCRYPT_MODE, secretKey,
                cipherDetails.getEncryptionParameterSpec(iv));
        byte[] chunk1 = cipher.update(plainTextChunks[0]);
        byte[] chunk2;
        final byte[] serializedContent;

        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
             Output output = new Output(out, 1_000_000)) {
            kryo.writeObject(output, cipher);
            serializedContent = out.toByteArray();
        }

        byte[] branchedChunk = new byte[0];
        if (verifyStateSeparation) {
            branchedChunk = cipher.doFinal(plainTextChunks[1]);
        }

        try (Input input = new FastInput(serializedContent)) {
            Cipher deserialized = kryo.readObject(input, Cipher.class);
            chunk2 = deserialized.doFinal(plainTextChunks[1]);
        }

        byte[] actual = combineArray(chunk1, chunk2);

        AssertJUnit.assertArrayEquals(
                "Deserialized cipher couldn't compute equivalent value",
                expected, actual);

        if (verifyStateSeparation) {
            byte[] branchedActual = combineArray(chunk1, branchedChunk);
            AssertJUnit.assertArrayEquals(
                    "Original cipher state affected by deserialized instance",
                    expected, branchedActual);
        }
    }

    private SecretKey loadSecretKey(final SupportedCipherDetails cipherDetails) {
        return SecretKeyUtils.loadKey(SECRET_KEY_BYTES, cipherDetails);
    }

    public static byte[] combineArray(final byte[] arr1, final byte... arr2) {
        final byte[] combinedArray = new byte[arr1.length + arr2.length];
        System.arraycopy(arr1, 0, combinedArray, 0, arr1.length);
        System.arraycopy(arr2, 0, combinedArray, arr1.length, arr2.length);
        return combinedArray;
    }
}
