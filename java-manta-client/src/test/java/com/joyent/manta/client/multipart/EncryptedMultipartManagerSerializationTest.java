/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.crypto.SupportedHmacsLookupMap;
import com.joyent.manta.config.DefaultsConfigContext;
import org.bouncycastle.crypto.Mac;
import org.bouncycastle.crypto.macs.HMac;
import org.bouncycastle.crypto.params.KeyParameter;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.crypto.SecretKey;
import java.io.ByteArrayOutputStream;
import java.util.Base64;

/**
 * Unit tests that attempt to serialize objects in the graph of a
 * {@link EncryptedMultipartUpload} class.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
@Test
public class EncryptedMultipartManagerSerializationTest {
    private SecretKey secretKey;
    private SupportedCipherDetails cipherDetails;
    private Kryo kryo = new Kryo();

    @BeforeClass
    public void setup() {
//        Kryo.DefaultInstantiatorStrategy defaultInstantiatorStrategy = new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy());
//
//        kryo.setInstantiatorStrategy(defaultInstantiatorStrategy);
//        Registration macRegistration = kryo.register(HMac.class, new MacSerializer(kryo));
        kryo.register(MacState.class, new JavaSerializer());
        this.cipherDetails = DefaultsConfigContext.DEFAULT_CIPHER;
        byte[] keyBytes = Base64.getDecoder().decode("qAnCNUmmFjUTtImNGv241Q==");
        this.secretKey = SecretKeyUtils.loadKey(keyBytes, cipherDetails);
    }

    public void serializeHmacMd5() throws Exception {
        final byte[] dataIn1 = new byte[] { (byte)4, (byte)83, (byte)113, (byte)66 };
        final byte[] dataIn2 = new byte[] { (byte)4, (byte)83, (byte)113, (byte)66 };
        final byte[] expected;
        final byte[] actual;

        {
            Mac hmac = SupportedHmacsLookupMap.INSTANCE.get("HmacMD5").get();
            hmac.init(new KeyParameter(this.secretKey.getEncoded()));
            hmac.update(dataIn1, 0, dataIn1.length);
            hmac.update(dataIn1, 0, dataIn2.length);
            expected = new byte[hmac.getMacSize()];
            actual = new byte[hmac.getMacSize()];
            hmac.doFinal(expected, 0);
        }

        HMac hmac = (HMac)SupportedHmacsLookupMap.INSTANCE.get("HmacMD5").get();
        hmac.init(new KeyParameter(this.secretKey.getEncoded()));
        hmac.update(dataIn1, 0, dataIn1.length);

        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
             Output output = new Output(out)) {
            kryo.writeObject(output, new MacState(hmac));

            try (Input input = new ByteBufferInput(out.toByteArray())) {
                MacState deserializedState = kryo.readObject(input, MacState.class);
                HMac deserialized = deserializedState.newInstanceFromState();

                deserialized.update(dataIn2, 0, dataIn2.length);
                deserialized.doFinal(actual, 0);

                AssertJUnit.assertArrayEquals(
                        "Deserialized hmac couldn't compute equivalent value",
                        expected, actual);
            }
        }
    }
}
