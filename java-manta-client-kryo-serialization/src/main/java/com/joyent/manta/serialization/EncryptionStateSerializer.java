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
import com.joyent.manta.client.crypto.EncryptingEntityHelper;
import com.joyent.manta.client.crypto.EncryptionContext;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.multipart.EncryptionState;
import com.joyent.manta.client.multipart.MultipartOutputStream;
import com.joyent.manta.util.HmacOutputStream;
import org.bouncycastle.crypto.macs.HMac;
import org.bouncycastle.jcajce.io.CipherOutputStream;
import org.objenesis.instantiator.sun.MagicInstantiator;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.Objects;

import static com.joyent.manta.serialization.ReflectionUtils.readField;
import static com.joyent.manta.serialization.ReflectionUtils.writeField;

/**
 * Kryo serializer that deconstructs a {@link EncryptionState} class for serialization / deserialization.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class EncryptionStateSerializer extends AbstractManualSerializer<EncryptionState> {

    private Field encryptionContextField = captureField("encryptionContext");
    private Field lastPartNumberField = captureField("lastPartNumber");
    private Field multipartStreamField = captureField("multipartStream");
    private Field cipherStreamField = captureField("cipherStream");
    private Field lastPartAuthWrittenField = captureField("lastPartAuthWritten");

    /**
     * Secret key to inject into encryption context.
     */
    private final SecretKey secretKey;

    /**
     * Creates a new serializer instance.
     *
     * @param kryo Kryo instance
     * @param secretKey secret key to inject into encryption context
     */
    public EncryptionStateSerializer(final Kryo kryo,
                                     final SecretKey secretKey) {
        super(EncryptionState.class, false);
        this.secretKey = secretKey;
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
        kryo.register(HMac.class, new HmacSerializer(kryo));

        final Class<?> closeShieldStreamClass = findCloseShieldStreamClass();
        Objects.requireNonNull(closeShieldStreamClass,
                "CloseShieldOutputStream reference must not be null");

        kryo.register(closeShieldStreamClass)
                .setInstantiator(new MagicInstantiator<>(closeShieldStreamClass));
        kryo.register(CipherOutputStream.class)
                .setInstantiator(new MagicInstantiator<>(CipherOutputStream.class));
    }

    /**
     * Utility method that searches for the conventional package path and
     * the shaded package path for an Apache Commons IO <code>CloseShieldOutputStream</code>
     * class. The class that is found is returned. We do this because shaded libraries
     * do not play nice with IDEs and unshaded libraries do not play nice with
     * downstream dependencies.
     *
     * @return <code>CloseShieldOutputStream</code> class reference
     */
    private Class<?> findCloseShieldStreamClass() {
        Class<?> unshadedCloseShieldStream = ReflectionUtils.findClassOrNull(
                "org.apache.commons.io.output.CloseShieldOutputStream");

        if (unshadedCloseShieldStream != null) {
            return unshadedCloseShieldStream;
        } else {
            return ReflectionUtils.findClassOrNull(
                    "com.joyent.manta.org.apache.commons.io.output.CloseShieldOutputStream");

        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(final Kryo kryo, final Output output, final EncryptionState object) {
        kryo.writeClassAndObject(output, readField(encryptionContextField, object));

        final int lastPartNumber = (int)readField(lastPartNumberField, object);
        output.writeInt(lastPartNumber, true);

        final boolean lastPartAuthWritten = (boolean)readField(lastPartAuthWrittenField, object);
        output.writeBoolean(lastPartAuthWritten);

        MultipartOutputStream multipartStream = (MultipartOutputStream)readField(multipartStreamField, object);
        ByteArrayOutputStream multipartStreamBuf;

        if (multipartStream == null) {
            multipartStreamBuf = null;
        } else {
            multipartStreamBuf = multipartStream.getBuf();
        }

        kryo.writeClassAndObject(output, multipartStreamBuf);

        Object cipherStream = readField(cipherStreamField, object);

        final HMac hmac;

        Objects.requireNonNull(cipherStream, "Cipher stream must not be null");

        if (cipherStream.getClass().equals(HmacOutputStream.class)) {
            HmacOutputStream hmacStream = (HmacOutputStream)cipherStream;
            hmac = hmacStream.getHmac();
        } else {
            hmac = null;
        }

        kryo.writeObjectOrNull(output, hmac, HMac.class);

        output.flush();
    }

    @Override
    @SuppressWarnings("unchecked")
    public EncryptionState read(final Kryo kryo, final Input input, final Class<EncryptionState> type) {
        final EncryptionContext encryptionContext = (EncryptionContext)kryo.readClassAndObject(input);
        final int lastPartNumber = input.readVarInt(true);
        final boolean lastPartAuthWritten = input.readBoolean();
        encryptionContext.setKey(secretKey);

        final EncryptionState encryptionState = newInstance();

        writeField(encryptionContextField, encryptionState, encryptionContext);
        writeField(lastPartNumberField, encryptionState, lastPartNumber);
        writeField(lastPartAuthWrittenField, encryptionState, lastPartAuthWritten);

        final int blockSize = encryptionContext.getCipherDetails().getBlockSizeInBytes();
        Object bufferObject = kryo.readClassAndObject(input);

        ByteArrayOutputStream multipartStreamBuffer;

        if (bufferObject == null) {
            multipartStreamBuffer = new ByteArrayOutputStream(blockSize);
        } else {
            multipartStreamBuffer = (ByteArrayOutputStream)bufferObject;
        }

        final MultipartOutputStream multipartStream = new MultipartOutputStream(blockSize, multipartStreamBuffer);
        writeField(multipartStreamField, encryptionState, multipartStream);

        final HMac hmac = kryo.readObjectOrNull(input, HMac.class);

        final OutputStream cipherStream = EncryptingEntityHelper.makeCipherOutputForStream(
                multipartStream, encryptionContext.getCipherDetails(), encryptionContext.getCipher(), hmac);

        writeField(cipherStreamField, encryptionState, cipherStream);

        return encryptionState;
    }
}
