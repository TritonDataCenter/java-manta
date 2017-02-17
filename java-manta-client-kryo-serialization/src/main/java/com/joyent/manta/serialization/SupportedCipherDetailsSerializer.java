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
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.crypto.SupportedCiphersLookupMap;

/**
 * Serializer class for {@link SupportedCipherDetails} implementations
 * that only serializes the name of the cipher id. Deserialization is done
 * by looking up the id against the values in {@link SupportedCiphersLookupMap}.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class SupportedCipherDetailsSerializer extends Serializer<SupportedCipherDetails> {
    /**
     * Creates a new serializer instance.
     */
    public SupportedCipherDetailsSerializer() {
        super(false);
    }

    @Override
    public void write(final Kryo kryo,
                      final Output output,
                      final SupportedCipherDetails object) {
        output.writeString(object.getCipherId());
    }

    @Override
    public SupportedCipherDetails read(final Kryo kryo,
                                       final Input input,
                                       final Class<SupportedCipherDetails> type) {
        final String cipherId = input.readString();
        return SupportedCiphersLookupMap.INSTANCE.get(cipherId);
    }
}
