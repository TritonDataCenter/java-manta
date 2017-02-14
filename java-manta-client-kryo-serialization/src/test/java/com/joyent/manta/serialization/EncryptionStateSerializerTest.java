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
import org.objenesis.instantiator.ObjectInstantiator;
import org.objenesis.strategy.InstantiatorStrategy;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.crypto.SecretKey;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Base64;

@Test
public class EncryptionStateSerializerTest {
    private Kryo kryo = new Kryo();
    private EncryptionState encryptionState;

    @BeforeClass
    public void setup() {
        SupportedCipherDetails cipherDetails = DefaultsConfigContext.DEFAULT_CIPHER;
        byte[] keyBytes = Base64.getDecoder().decode("qAnCNUmmFjUTtImNGv241Q==");
        SecretKey secretKey = SecretKeyUtils.loadKey(keyBytes, cipherDetails);
        EncryptionContext encryptionContext = new EncryptionContext(secretKey, cipherDetails);
        this.encryptionState = new EncryptionState(encryptionContext);
    }

    public void canSerializeEncryptionState() throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Output output = new Output(outputStream);
        this.kryo.writeObject(output, this.encryptionState);
        output.close();

        Input input = new Input(outputStream.toByteArray());
        EncryptionState deserializedEncryptionState = kryo.readObject(input, EncryptionState.class);
        input.close();

        //AssertJUnit.assertEquals(encryptionState.getEncryptionContext().getSecretKey(), deserializedEncryptionState.getEncryptionContext().getSecretKey());
    }
}
