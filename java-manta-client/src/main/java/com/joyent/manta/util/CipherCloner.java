/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import com.joyent.manta.exception.MantaReflectionException;
import com.rits.cloning.Cloner;

import javax.crypto.Cipher;
import javax.crypto.CipherSpi;
import javax.crypto.NoSuchPaddingException;

import java.lang.reflect.Field;
import java.security.NoSuchAlgorithmException;

import static org.apache.commons.lang3.reflect.FieldUtils.getField;
import static org.apache.commons.lang3.reflect.FieldUtils.readField;
import static org.apache.commons.lang3.reflect.FieldUtils.writeField;

public final class CipherCloner extends AbstractCloner<Cipher> {

    private static Field cryptoPermField = getField(Cipher.class, "cryptoPerm", true);
    private static Field exmechField = getField(Cipher.class, "exmech", true);
    private static Field firstServiceField = getField(Cipher.class, "firstService", true);
    private static Field firstSpiField = getField(Cipher.class, "firstSpi", true);
    private static Field initializedField = getField(Cipher.class, "initialized", true);
    private static Field lockField = getField(Cipher.class, "lock", true);
    private static Field providerField = getField(Cipher.class, "provider", true);
    private static Field opmodeField = getField(Cipher.class, "opmode", true);
    private static Field serviceIteratorField = getField(Cipher.class, "serviceIterator", true);
    private static Field spiField = getField(Cipher.class, "spi", true);
    private static Field transformationField = getField(Cipher.class, "transformation", true);
    private static Field transformsField = getField(Cipher.class, "transforms", true);

    public Cipher clone(final Cipher source) {
        final String transformation;
        final Cipher cloned;
        try {
            transformation = (String) readField(transformationField, source);

            try {
                cloned = Cipher.getInstance(transformation);
            } catch (NoSuchPaddingException | NoSuchAlgorithmException e) {
                throw new MantaReflectionException("Unexpected exception while cloning Cipher.", e);
            }

            overwrite(source, cloned);
        } catch (IllegalAccessException e) {
            throw new MantaReflectionException(e);
        }

        return cloned;
    }

    public void overwrite(final Cipher source, final Cipher target) {
        try {
            cloneField(cryptoPermField, source, target);
            cloneField(exmechField, source, target);
            cloneField(firstServiceField, source, target);
            cloneField(initializedField, source, target);
            cloneField(lockField, source, target);
            cloneField(providerField, source, target);
            cloneField(opmodeField, source, target);
            cloneField(serviceIteratorField, source, target);
            cloneField(transformationField, source, target);
            cloneField(transformsField, source, target);
            cloneField(firstSpiField, source, target);

            writeField(spiField, target, new Cloner().deepClone(readField(spiField, source)));
        } catch (Exception e) {
            throw new MantaReflectionException(e);
        }
    }
}
