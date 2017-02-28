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
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.bouncycastle.jcajce.provider.symmetric.AES;
import org.objenesis.instantiator.sun.MagicInstantiator;

import javax.crypto.Cipher;
import java.lang.reflect.Field;
import java.security.GeneralSecurityException;
import java.security.Provider;
import java.security.Security;
import java.util.IdentityHashMap;

import static com.joyent.manta.serialization.ReflectionUtils.*;

/**
 * Kryo serializer that deconstructs a {@link Cipher} class and
 * allows for serialization / deserialization.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class CipherSerializer extends AbstractManualSerializer<Cipher> {
    /**
     * Reference to the private crypto all permission class.
     */
    private final Class<?> cryptoAllPermissionClass;

    private Field cryptoPermField = captureField("cryptoPerm");
    private Field exmechField = captureField("exmech");
    private Field firstServiceField = captureField("firstService");
    private Field firstSpiField = captureField("firstSpi");
    private Field initializedField = captureField("initialized");
    private Field lockField = captureField("lock");
    private Field providerField = captureField("provider");
    private Field opmodeField = captureField("opmode");
    private Field serviceIteratorField = captureField("serviceIterator");
    private Field spiField = captureField("spi");
    private Field transformationField = captureField("transformation");
    private Field transformsField = captureField("transforms");

    {
        cryptoAllPermissionClass = findClass("javax.crypto.CryptoAllPermission");
    }

    /**
     * Creates a new {@link Cipher} serializer instance.
     *
     * @param kryo Kryo instance
     */
    public CipherSerializer(final Kryo kryo) {
        super(Cipher.class, false);
        registerClasses(kryo);
    }

    /**
     * Registers the classes needed for serialization with Kryo.
     *
     * @param kryo Kryo instance
     */
    private void registerClasses(final Kryo kryo) {
        kryo.register(cryptoAllPermissionClass, new JavaSerializer());

        kryo.register(AES.ECB.class, new BaseBlockCipherSerializer<>(kryo, AES.ECB.class));
        kryo.register(AES.CBC.class, new BaseBlockCipherSerializer<>(kryo, AES.CBC.class));
        kryo.register(AES.GCM.class, new BaseBlockCipherSerializer<>(kryo, AES.GCM.class));

        // We just store the name of the provider and then get it by that name
        ProviderSerializer providerSerializer = new ProviderSerializer();

        // We register every available provider so that in can be serialized
        kryo.register(Provider.class, providerSerializer);

        for (Provider p : Security.getProviders()) {
            kryo.register(p.getClass(), providerSerializer);
        }

        // Everything below is for supporting serializing PKCS11 native ciphers

        kryo.register(IdentityHashMap.class, new JavaSerializer());

        Class<?> p11CipherClass = findClass("sun.security.pkcs11.P11Cipher");

        if (p11CipherClass != null) {
            kryo.register(p11CipherClass, new CompatibleFieldSerializer(kryo, p11CipherClass))
                    .setInstantiator(new MagicInstantiator<>(p11CipherClass));
        }

        Class<?> p11SecretKeyClass = findClass("sun.security.pkcs11.P11Key$P11SecretKey");

        if (p11SecretKeyClass != null) {
            kryo.register(p11SecretKeyClass, new JavaSerializer());
        }

        Class<?> sessionKeyRefClass = findClass("sun.security.pkcs11.SessionKeyRef");

        if (sessionKeyRefClass != null) {
            kryo.register(sessionKeyRefClass, new CompatibleFieldSerializer<>(
                    kryo, sessionKeyRefClass))
                    .setInstantiator(new MagicInstantiator<>(sessionKeyRefClass));
        }

        Class<?> sessionClass = findClass("sun.security.pkcs11.Session");

        if (sessionClass != null) {
            kryo.register(sessionClass, new CompatibleFieldSerializer<>(kryo, sessionClass))
                    .setInstantiator(new MagicInstantiator<>(sessionClass));
        }

        Class<?> sessionRefClass = findClass("sun.security.pkcs11.SessionRef");

        if (sessionKeyRefClass != null) {
            kryo.register(sessionRefClass, new SessionRefSerializer(kryo));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(final Kryo kryo, final Output output, final Cipher object) {
        final Object cryptoPerm = readField(cryptoPermField, object);
        kryo.writeObjectOrNull(output, cryptoPerm, cryptoAllPermissionClass);

        kryo.writeClassAndObject(output, readField(exmechField, object));
        kryo.writeClassAndObject(output, readField(firstServiceField, object));
        kryo.writeClassAndObject(output, readField(firstSpiField, object));
        output.writeBoolean((Boolean)readField(initializedField, object));
        kryo.writeClassAndObject(output, readField(lockField, object));

        final int opmode = (int)readField(opmodeField, object);
        output.writeInt(opmode, true);

        Provider provider = (Provider)readField(providerField, object);
        output.writeString(provider.getName());

        kryo.writeClassAndObject(output, readField(serviceIteratorField, object));

        final Object spi = readField(spiField, object);
        kryo.writeClassAndObject(output, spi);
        output.writeString((String)readField(transformationField, object));
        kryo.writeClassAndObject(output, readField(transformsField, object));

        output.flush();
    }

    @Override
    @SuppressWarnings("InsecureCryptoUsage")
    public Cipher read(final Kryo kryo, final Input input, final Class<Cipher> type) {
        final Object cryptoPerm = kryo.readObjectOrNull(input, cryptoAllPermissionClass);
        final Object exmech = kryo.readClassAndObject(input);
        final Object firstService = kryo.readClassAndObject(input);
        final Object firstSpi = kryo.readClassAndObject(input);
        final boolean initialized = input.readBoolean();
        final Object lock = kryo.readClassAndObject(input);
        final int opmode = input.readVarInt(true);

        final String providerName = input.readString();
        final Provider provider;
        if (providerName != null) {
            provider = Security.getProvider(providerName);
        } else {
            provider = null;
        }

        final Object serviceIterator = kryo.readClassAndObject(input);
        final Object spi = kryo.readClassAndObject(input);
        final String transformation = input.readString();
        final Object transforms = kryo.readClassAndObject(input);

        final Cipher cipher;

        try {
            cipher = Cipher.getInstance(transformation);
        } catch (GeneralSecurityException e) {
            String msg = "Unable to instantiate Cipher";
            MantaClientSerializationException mcse =  new MantaClientSerializationException(msg, e);
            mcse.setContextValue("algorithmName", transformation);
            throw mcse;
        }

        writeField(cryptoPermField, cipher, cryptoPerm);
        writeField(exmechField, cipher, exmech);
        writeField(firstServiceField, cipher, firstService);
        writeField(firstSpiField, cipher, firstSpi);
        writeField(initializedField, cipher, initialized);
        writeField(lockField, cipher, lock);
        writeField(opmodeField, cipher, opmode);
        writeField(providerField, cipher, provider);
        writeField(serviceIteratorField, cipher, serviceIterator);
        writeField(spiField, cipher, spi);
        writeField(transformationField, cipher, transformation);
        writeField(transformsField, cipher, transforms);

        return cipher;
    }
}
