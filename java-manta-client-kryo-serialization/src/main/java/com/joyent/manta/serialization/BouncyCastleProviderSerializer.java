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
import com.joyent.manta.client.crypto.BouncyCastleLoader;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

/**
 * A fake serializer that deserializes a BouncyCastle provider to the provider
 * in memory. The write() operation performs a NOOP and serializes nothing.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class BouncyCastleProviderSerializer extends Serializer<BouncyCastleProvider> {
    public BouncyCastleProviderSerializer() {
        super(false);
    }

    @Override
    public void write(final Kryo kryo, final Output output, final BouncyCastleProvider object) {
        // do nothing
    }

    @Override
    @SuppressWarnings("unchecked")
    public BouncyCastleProvider read(final Kryo kryo, final Input input, final Class<BouncyCastleProvider> type) {
        return (BouncyCastleProvider)BouncyCastleLoader.BOUNCY_CASTLE_PROVIDER;
    }
}
