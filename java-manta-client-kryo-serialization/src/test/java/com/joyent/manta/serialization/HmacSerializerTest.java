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
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.crypto.SupportedHmacsLookupMap;
import com.joyent.manta.config.DefaultsConfigContext;
import org.bouncycastle.crypto.macs.HMac;
import org.bouncycastle.crypto.params.KeyParameter;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.crypto.SecretKey;
import java.io.ByteArrayOutputStream;
import java.util.Base64;

/**
 * Unit tests that verify we can serialize and deserialize a {@link HMac}
 * object using the Kryo framework.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
@Test
public class HmacSerializerTest {
    private Kryo kryo = new Kryo();
    private SecretKey secretKey;

    @BeforeClass
    public void setup() {
        kryo.register(HMac.class, new HmacSerializer(kryo));
        SupportedCipherDetails cipherDetails = DefaultsConfigContext.DEFAULT_CIPHER;
        byte[] keyBytes = Base64.getDecoder().decode("qAnCNUmmFjUTtImNGv241Q==");
        this.secretKey = SecretKeyUtils.loadKey(keyBytes, cipherDetails);
    }

    public void canSerializeHmacMd5() throws Exception {
        canSerializeHmac("HmacMD5");
    }

    public void canSerializeHmacSha1() throws Exception {
        canSerializeHmac("HmacSHA1");
    }

    public void canSerializeHmacSha256() throws Exception {
        canSerializeHmac("HmacSHA256");
    }

    public void canSerializeHmacSha384() throws Exception {
        canSerializeHmac("HmacSHA384");
    }

    public void canSerializeHmacSha512() throws Exception {
        canSerializeHmac("HmacSHA512");
    }

    private void canSerializeHmac(final String algorithm) throws Exception {
        final byte[] dataIn1 = new byte[] { (byte)4, (byte)83, (byte)113, (byte)66 };
        final byte[] dataIn2 = new byte[] { (byte)4, (byte)83, (byte)113, (byte)66 };
        final byte[] expected;
        final byte[] actual;

        {
            HMac hmacExpected = SupportedHmacsLookupMap.INSTANCE.get(algorithm).get();
            hmacExpected.init(new KeyParameter(this.secretKey.getEncoded()));
            hmacExpected.update(dataIn1, 0, dataIn1.length);
            hmacExpected.update(dataIn1, 0, dataIn2.length);
            expected = new byte[hmacExpected.getMacSize()];
            actual = new byte[hmacExpected.getMacSize()];
            hmacExpected.doFinal(expected, 0);
        }

        HMac hmac = SupportedHmacsLookupMap.INSTANCE.get(algorithm).get();
        hmac.init(new KeyParameter(this.secretKey.getEncoded()));
        hmac.update(dataIn1, 0, dataIn1.length);

        final byte[] serializedContent;

        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
             Output output = new Output(out)) {
            kryo.writeObject(output, hmac);
            output.flush();

            serializedContent = out.toByteArray();
        }

        try (Input input = new FastInput(serializedContent)) {
            HMac deserialized = kryo.readObject(input, HMac.class);

            deserialized.update(dataIn2, 0, dataIn2.length);
            deserialized.doFinal(actual, 0);

            AssertJUnit.assertArrayEquals(
                    "Deserialized hmac couldn't compute equivalent value",
                    expected, actual);
        }
    }
}
