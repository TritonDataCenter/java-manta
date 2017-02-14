/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.joyent.manta.client.crypto.EncryptionContext;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.multipart.EncryptionState;
import com.joyent.manta.config.DefaultsConfigContext;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.objenesis.instantiator.ObjectInstantiator;
import org.objenesis.strategy.InstantiatorStrategy;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.crypto.SecretKey;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Base64;

@Test
public class EncryptionStateSerializerTest {
    private static final Field ENCRYPTION_CONTEXT_FIELD =
            FieldUtils.getField(EncryptionState.class, "encryptionContext", true);

    private byte[] keyBytes = Base64.getDecoder().decode("qAnCNUmmFjUTtImNGv241Q==");
    SupportedCipherDetails cipherDetails = DefaultsConfigContext.DEFAULT_CIPHER;
    SecretKey secretKey = SecretKeyUtils.loadKey(keyBytes, cipherDetails);
    private Kryo kryo = new Kryo();
    private EncryptionState encryptionState;

    @BeforeClass
    public void setup() {
        kryo.register(EncryptionStateSerializer.class, new EncryptionStateSerializer(kryo));
        EncryptionContext encryptionContext = new EncryptionContext(secretKey, cipherDetails);
        this.encryptionState = new EncryptionState(encryptionContext);
    }

    @SuppressWarnings("unchecked")
    public void canSerializeEncryptionState() throws Exception {
        final byte[] serializedData;
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
             Output output = new Output(outputStream)) {
            this.kryo.writeObject(output, this.encryptionState);
            output.flush();
            serializedData = outputStream.toByteArray();
        }

        try (Input input = new Input(serializedData)) {
            EncryptionState actual = kryo.readObject(input, EncryptionState.class);
            EncryptionContext actualEncryptionContext =
                    (EncryptionContext)FieldUtils.readField(ENCRYPTION_CONTEXT_FIELD, actual, true);
            actualEncryptionContext.setKey(secretKey);

            Assert.assertEquals(actual, encryptionState);
        }
    }
}
