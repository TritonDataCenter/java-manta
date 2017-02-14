/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.joyent.manta.client.crypto.BouncyCastleLoader;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.bouncycastle.jcajce.provider.symmetric.AES;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import java.lang.reflect.Field;
import java.security.GeneralSecurityException;
import java.util.Base64;

/**
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class CipherSerializer extends Serializer<Cipher> {
    private static final Logger logger = LoggerFactory.getLogger(CipherSerializer.class);

    private static final Class<?> CRYPTO_ALL_PERMISSION_CLASS;

    private static Field CRYPTO_PERM_FIELD = captureField("cryptoPerm");
    private static Field EXMECH_FIELD = captureField("exmech");
    private static Field FIRST_SERVICE_FIELD = captureField("firstService");
    private static Field FIRST_SPI = captureField("firstSpi");
    private static Field INITIALIZED_FIELD = captureField("initialized");
    private static Field LOCK_FIELD = captureField("lock");
    private static Field OPMODE_FIELD = captureField("opmode");
    private static Field PROVIDER_FIELD = captureField("provider");
    private static Field SERVICE_ITERATOR_FIELD = captureField("serviceIterator");
    private static Field SPI_FIELD = captureField("spi");
    private static Field TRANSFORMATION_FIELD = captureField("transformation");
    private static Field TRANSFORMS_FIELD = captureField("transforms");

    static {
        CRYPTO_ALL_PERMISSION_CLASS = findClass("javax.crypto.CryptoAllPermission");
    }

    public CipherSerializer(final Kryo kryo) {
        super(false);
        registerClasses(kryo);
    }

    private void registerClasses(final Kryo kryo) {
        Class<?> cryptoAllPermissionClass = findClass("javax.crypto.CryptoAllPermission");
        kryo.register(cryptoAllPermissionClass, new JavaSerializer());

        kryo.register(AES.ECB.class, new BaseBlockCipherSerializer<>(kryo, AES.ECB.class));
        kryo.register(AES.CBC.class, new BaseBlockCipherSerializer<>(kryo, AES.CBC.class));
        kryo.register(AES.GCM.class, new BaseBlockCipherSerializer<>(kryo, AES.GCM.class));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(Kryo kryo, Output output, Cipher object) {
        logger.debug("[Kryo] Writing cipher object, algorithm={}, iv={}", object.getAlgorithm(), Base64.getEncoder().encodeToString(object.getIV()));

        final Object cryptoPerm = readField(CRYPTO_PERM_FIELD, object);
        kryo.writeObjectOrNull(output, cryptoPerm, CRYPTO_ALL_PERMISSION_CLASS);

        kryo.writeClassAndObject(output, readField(EXMECH_FIELD, object));
        kryo.writeClassAndObject(output, readField(FIRST_SERVICE_FIELD, object));
        kryo.writeClassAndObject(output, readField(FIRST_SPI, object));
        output.writeBoolean((Boolean)readField(INITIALIZED_FIELD, object));
        kryo.writeClassAndObject(output, readField(LOCK_FIELD, object));

        final int opmode = (int)readField(OPMODE_FIELD, object);
        output.writeInt(opmode, true);

        kryo.writeClassAndObject(output, readField(SERVICE_ITERATOR_FIELD, object));

        final Object spi = readField(SPI_FIELD, object);
        kryo.writeClassAndObject(output, spi);
        output.writeString((String)readField(TRANSFORMATION_FIELD, object));
        kryo.writeClassAndObject(output, readField(TRANSFORMS_FIELD, object));

        output.flush();
    }

    @Override
    @SuppressWarnings("InsecureCryptoUsage")
    public Cipher read(Kryo kryo, Input input, Class<Cipher> type) {
        final Object cryptoPerm = kryo.readObjectOrNull(input, CRYPTO_ALL_PERMISSION_CLASS);
        final Object exmech = kryo.readClassAndObject(input);
        final Object firstService = kryo.readClassAndObject(input);
        final Object firstSpi = kryo.readClassAndObject(input);
        final boolean initialized = input.readBoolean();
        final Object lock = kryo.readClassAndObject(input);
        final int opmode = input.readVarInt(true);
        final Object serviceIterator = kryo.readClassAndObject(input);
        final Object spi = kryo.readClassAndObject(input);
        final String transformation = input.readString();
        final Object transforms = kryo.readClassAndObject(input);

        final Cipher cipher;

        try {
            cipher = Cipher.getInstance(transformation,
                    BouncyCastleLoader.BOUNCY_CASTLE_PROVIDER);
        } catch (GeneralSecurityException e) {
            String msg = String.format("Unable to instantiate Cipher [%s]",
                    transformation);
            throw new SerializationException(msg, e);
        }

        writeField(CRYPTO_PERM_FIELD, cipher, cryptoPerm);
        writeField(EXMECH_FIELD, cipher, exmech);
        writeField(FIRST_SERVICE_FIELD, cipher, firstService);
        writeField(FIRST_SPI, cipher, firstSpi);
        writeField(INITIALIZED_FIELD, cipher, initialized);
        writeField(LOCK_FIELD, cipher, lock);
        writeField(OPMODE_FIELD, cipher, opmode);
        writeField(SERVICE_ITERATOR_FIELD, cipher, serviceIterator);
        writeField(SPI_FIELD, cipher, spi);
        writeField(TRANSFORMATION_FIELD, cipher, transformation);
        writeField(TRANSFORMS_FIELD, cipher, transforms);

        return cipher;
    }

    private static Field captureField(final String fieldName) {
        return FieldUtils.getField(Cipher.class, fieldName, true);
    }

    private static Object readField(final Field field, Cipher object) {
        try {
        return FieldUtils.readField(field, object, true);
        } catch (IllegalAccessException e) {
            String msg = String.format("Error reading private field from [%s] class",
                    object.getClass().getName());
            throw new SerializationException(msg);
        }
    }

    private void writeField(final Field field, final Object target, final Object value) {
        try {
            FieldUtils.writeField(field, target, value);
        } catch (IllegalAccessException e) {
            String msg = String.format("Unable to write value [%s] to field [%s]",
                    value, field);
            throw new SerializationException(msg, e);
        }
    }

    private static Class<?> findClass(final String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            String msg = String.format("Class not found in class path: %s",
                    className);
            throw new UnsupportedOperationException(msg, e);
        }
    }
}
