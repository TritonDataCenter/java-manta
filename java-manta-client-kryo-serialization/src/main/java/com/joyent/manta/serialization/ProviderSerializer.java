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

import java.security.Provider;
import java.security.Security;

/**
 * Serializer class that writes out the name of a {@link Provider} without
 * writing its state.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class ProviderSerializer extends Serializer<Provider> {
    /**
     * Creates a new instance.
     */
    public ProviderSerializer() {
        super(false, true);
    }

    @Override
    public void write(final Kryo kryo, final Output output, final Provider object) {
        final String name = object.getName();
        output.writeString(name);
    }

    @Override
    public Provider read(final Kryo kryo, final Input input, final Class<Provider> type) {
        final String name = input.readString();
        return Security.getProvider(name);
    }
}
