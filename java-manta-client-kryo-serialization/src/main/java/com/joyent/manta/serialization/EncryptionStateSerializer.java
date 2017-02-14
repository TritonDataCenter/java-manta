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
import com.joyent.manta.client.crypto.AesCbcCipherDetails;
import com.joyent.manta.client.crypto.AesCtrCipherDetails;
import com.joyent.manta.client.crypto.AesGcmCipherDetails;
import com.joyent.manta.client.crypto.EncryptionContext;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.multipart.EncryptionState;
import org.objenesis.instantiator.sun.MagicInstantiator;

import javax.crypto.Cipher;
import java.lang.reflect.Field;

/**
 * Kryo serializer that deconstructs a {@link EncryptionState} class for serialization / deserialization.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class EncryptionStateSerializer extends AbstractManualSerializer<EncryptionState> {

    private Field encryptionContextField = captureField("encryptionContext");
    private Field lockField = captureField("lock");
    private Field lastPartNumberField = captureField("lastPartNumber");
    private Field multipartStreamField = captureField("multipartStream");

    /**
     * Creates a new serializer instance.
     *
     * @param kryo Kryo instance
     */
    public EncryptionStateSerializer(final Kryo kryo) {
        super(EncryptionState.class, false);
        registerClasses(kryo);
    }

    /**
     * Registers the classes needed for serialization with Kryo.
     *
     * @param kryo Kryo instance
     */
    private void registerClasses(final Kryo kryo) {
        kryo.register(EncryptionContext.class)
                .setInstantiator(new MagicInstantiator<>(EncryptionContext.class));
        kryo.register(SupportedCipherDetails.class, new SupportedCipherDetailsSerializer());
        kryo.register(AesCtrCipherDetails.class, new SupportedCipherDetailsSerializer());
        kryo.register(AesCbcCipherDetails.class, new SupportedCipherDetailsSerializer());
        kryo.register(AesGcmCipherDetails.class, new SupportedCipherDetailsSerializer());
        kryo.register(Cipher.class, new CipherSerializer(kryo));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(final Kryo kryo, final Output output, final EncryptionState object) {
        kryo.writeClassAndObject(output, readField(encryptionContextField, object));
        kryo.writeClassAndObject(output, readField(lockField, object));

        final int lastPartNumber = (int)readField(lastPartNumberField, object);
        output.writeInt(lastPartNumber, true);

        kryo.writeClassAndObject(output, readField(multipartStreamField, object));

        output.flush();
    }

    @Override
    public EncryptionState read(final Kryo kryo, final Input input, final Class<EncryptionState> type) {
        final Object encryptionContext = kryo.readClassAndObject(input);
        final Object lock = kryo.readClassAndObject(input);
        final int lastPartNumber = input.readVarInt(true);
        final Object multipartStream = kryo.readClassAndObject(input);

        final EncryptionState encryptionState = newInstance();

        writeField(encryptionContextField, encryptionState, encryptionContext);
        writeField(lockField, encryptionState, lock);
        writeField(lastPartNumberField, encryptionState, lastPartNumber);
        writeField(multipartStreamField, encryptionState, multipartStream);

        return encryptionState;
    }
}
