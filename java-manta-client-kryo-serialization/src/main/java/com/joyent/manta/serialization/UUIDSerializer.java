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

import java.util.UUID;

/**
 * Kryo serializer class that serializes {@link UUID} instances
 * as two longs.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class UUIDSerializer extends Serializer<UUID> {
    /**
     * Creates a new serializer instance.
     */
    public UUIDSerializer() {
        super(false);
    }

    @Override
    public void write(final Kryo kryo, final Output output, final UUID object) {
        output.writeVarLong(object.getMostSignificantBits(), false);
        output.writeVarLong(object.getLeastSignificantBits(), false);

        output.flush();
    }

    @Override
    public UUID read(final Kryo kryo, final Input input, final Class<UUID> type) {
        final long mostSignificantBits = input.readVarLong(false);
        final long leastSignificantBits = input.readVarLong(false);

        return new UUID(mostSignificantBits, leastSignificantBits);
    }
}
