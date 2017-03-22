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

import java.util.UUID;

/**
 * Kryo serializer for instances that extend {@link AbstractMultipartUpload}.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 *
 * @param <T> type to serialize
 */
public class MultipartUploadSerializer<T extends AbstractMultipartUpload> extends FieldSerializer<T> {
    /**
     * Creates new serializer instance.
     * @param kryo Kryo instance
     * @param type type of instance to serialize
     */
    public MultipartUploadSerializer(final Kryo kryo, final Class<?> type) {
        super(kryo, type);
        registerClasses(kryo);
    }

    /**
     * Registers the classes needed for serialization with Kryo.
     *
     * @param kryo Kryo instance
     */
    private void registerClasses(final Kryo kryo) {
        kryo.register(UUID.class, new UUIDSerializer());
    }
}
