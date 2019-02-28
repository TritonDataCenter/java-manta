/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.joyent.manta.client.multipart.AbstractMultipartUpload;
import com.joyent.manta.client.multipart.EncryptedMultipartUpload;
import com.joyent.manta.client.multipart.EncryptionState;

import javax.crypto.SecretKey;

/**
 * Kryo serializer for serializing {@link EncryptedMultipartUpload}
 * instances.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 *
 * @param <WRAPPED> underlying {@link AbstractMultipartUpload} generic type of serialized class
 */
public class EncryptedMultipartSerializer<WRAPPED extends AbstractMultipartUpload>
        extends FieldSerializer<EncryptedMultipartUpload<WRAPPED>> {

    /**
     * Secret key to inject upon deserialization.
     */
    private SecretKey secretKey;

    /**
     * Class of underlying {@link AbstractMultipartUpload} generic type of
     * serialized class.
     */
    private Class<WRAPPED> wrappedType;

    /**
     * Creates a new serializer instance.
     *
     * @param kryo Kryo instance
     * @param type type of instance to serialize
     * @param wrappedType type of wrapped type of serialized class
     * @param secretKey secret key to inject on deserialize
     */
    public EncryptedMultipartSerializer(final Kryo kryo,
                                        final Class<?> type,
                                        final Class<WRAPPED> wrappedType,
                                        final SecretKey secretKey) {
        super(kryo, type);
        this.wrappedType = wrappedType;
        this.secretKey = secretKey;

        registerClasses(kryo);
    }

    /**
     * Registers the classes needed for serialization with Kryo.
     *
     * @param kryo Kryo instance
     */
    private void registerClasses(final Kryo kryo) {
        kryo.register(EncryptionState.class, new EncryptionStateSerializer(kryo, secretKey));
        kryo.register(wrappedType, new MultipartUploadSerializer<WRAPPED>(kryo, wrappedType));
    }
}
