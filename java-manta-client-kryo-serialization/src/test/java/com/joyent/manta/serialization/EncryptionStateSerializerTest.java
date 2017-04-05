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
import com.joyent.manta.client.crypto.EncryptingEntityHelper;
import com.joyent.manta.client.crypto.EncryptionContext;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.multipart.EncryptionState;
import com.joyent.manta.client.multipart.MultipartOutputStream;
import com.joyent.manta.config.DefaultsConfigContext;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.crypto.SecretKey;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.Base64;

@Test
public class EncryptionStateSerializerTest {
    private static final Field ENCRYPTION_CONTEXT_FIELD =
            ReflectionUtils.getField(EncryptionState.class, "encryptionContext");

    private byte[] keyBytes = Base64.getDecoder().decode("qAnCNUmmFjUTtImNGv241Q==");
    SupportedCipherDetails cipherDetails = DefaultsConfigContext.DEFAULT_CIPHER;
    SecretKey secretKey = SecretKeyUtils.loadKey(keyBytes, cipherDetails);
    private Kryo kryo = new Kryo();

    @BeforeClass
    public void setup() {
        kryo.register(EncryptionState.class,
                new EncryptionStateSerializer(kryo, secretKey));
    }

    @SuppressWarnings("unchecked")
    public void canSerializeEncryptionState() throws Exception {
        final EncryptionState encryptionState = newEncryptionStateInstance();

        final byte[] serializedData;
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
             Output output = new Output(outputStream)) {
            this.kryo.writeObject(output, encryptionState);
            output.flush();
            serializedData = outputStream.toByteArray();
        }

        try (Input input = new Input(serializedData)) {
            EncryptionState actual = kryo.readObject(input, EncryptionState.class);
            EncryptionContext actualEncryptionContext =
                    (EncryptionContext)ReflectionUtils.readField(ENCRYPTION_CONTEXT_FIELD, actual);
            actualEncryptionContext.setKey(secretKey);

            Assert.assertEquals(actual, encryptionState);
        }
    }

    private EncryptionState newEncryptionStateInstance() {
        EncryptionContext encryptionContext = new EncryptionContext(secretKey, cipherDetails);
        EncryptionState encryptionState = new EncryptionState(encryptionContext);

        MultipartOutputStream multipartStream = new MultipartOutputStream(
                cipherDetails.getBlockSizeInBytes());

        Field multipartStreamField = ReflectionUtils.getField(EncryptionState.class,
                "multipartStream");
        Field cipherStreamField = ReflectionUtils.getField(EncryptionState.class,
                "cipherStream");
        Field lastPartAuthWrittenField = ReflectionUtils.getField(EncryptionState.class,
                "lastPartAuthWritten");

        try {
            FieldUtils.writeField(multipartStreamField, encryptionState, multipartStream);

            final OutputStream cipherStream = EncryptingEntityHelper.makeCipherOutputForStream(
                    multipartStream, encryptionContext);

            FieldUtils.writeField(cipherStreamField, encryptionState, cipherStream);
            FieldUtils.writeField(lastPartAuthWrittenField, encryptionState, true);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }

        return encryptionState;
    }
}
