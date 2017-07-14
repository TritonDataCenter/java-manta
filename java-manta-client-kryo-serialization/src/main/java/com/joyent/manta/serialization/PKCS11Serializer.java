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
import sun.security.pkcs11.wrapper.PKCS11;
import sun.security.pkcs11.wrapper.PKCS11Exception;

import java.io.IOException;
import java.lang.reflect.Field;

/**
 * Kryo serializer class that serializes {@link PKCS11} instances.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class PKCS11Serializer extends AbstractManualSerializer<PKCS11> {
    /**
     * Name of field that identifies the PKCS11 module.
     */
    private static final Field MODULE_PATH_FIELD = ReflectionUtils
            .getField(PKCS11.class, "pkcs11ModulePath");

    /**
     * Creates a new serializer instance.
     */
    public PKCS11Serializer() {
        super(PKCS11.class, false, true);
    }

    @Override
    public void write(final Kryo kryo, final Output output, final PKCS11 object) {
        Object modulePath = ReflectionUtils.readField(MODULE_PATH_FIELD,
                object);
        output.writeString(modulePath.toString());
    }

    @Override
    public PKCS11 read(final Kryo kryo, final Input input, final Class<PKCS11> type) {
        String modulePath = input.readString();

        try {
            return PKCS11.getInstance(modulePath, "C_GetFunctionList", null, false);
        } catch (IOException | PKCS11Exception e) {
            throw new MantaClientSerializationException(
                    "Unable to instantiate PKC11 class", e);
        }
    }
}
