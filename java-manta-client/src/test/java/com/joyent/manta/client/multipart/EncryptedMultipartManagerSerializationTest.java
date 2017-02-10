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
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.crypto.SupportedHmacsLookupMap;
import com.joyent.manta.config.DefaultsConfigContext;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.crypto.Mac;
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
        this.cipherDetails = DefaultsConfigContext.DEFAULT_CIPHER;
        byte[] keyBytes = Base64.getDecoder().decode("qAnCNUmmFjUTtImNGv241Q==");
        this.secretKey = SecretKeyUtils.loadKey(keyBytes, cipherDetails);
    }

    public void serializeHmacMd5() throws Exception {
        final byte[] dataIn1 = new byte[] { (byte)4, (byte)83, (byte)113, (byte)66 };
        final byte[] dataIn2 = new byte[] { (byte)4, (byte)83, (byte)113, (byte)66 };
        final byte[] expected;

        {
            Mac hmac = SupportedHmacsLookupMap.INSTANCE.get("HmacMD5").get();
            hmac.init(this.secretKey);
            hmac.update(dataIn1);
            expected = hmac.doFinal(dataIn2);
        }

        Mac hmac = SupportedHmacsLookupMap.INSTANCE.get("HmacMD5").get();
        hmac.init(this.secretKey);
        hmac.update(dataIn1);

        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
             Output output = new Output(out)) {
            kryo.writeObject(output, hmac);

            try (Input input = new ByteBufferInput(out.toByteArray())) {
                Mac deserialized = kryo.readObject(input, Mac.class);
                byte[] actual = deserialized.doFinal(dataIn2);

                AssertJUnit.assertArrayEquals(
                        "Deserialized hmac couldn't compute equivalent value",
                        expected, actual);
            }
        }
    }
}
