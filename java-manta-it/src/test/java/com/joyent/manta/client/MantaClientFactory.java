/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.config.ConfigContext;

/**
 * TODO: this might belong in the main package?
 */
public final class MantaClientFactory {

    private MantaClientFactory() {
    }

    public static MantaClient build(final ConfigContext config) {
        return new MantaClient(config);
    }

    public static MantaClient build(final ConfigContext config, final Boolean lazy) {
        if (lazy != null && lazy) {
            return new LazyMantaClient(config);
        }

        return build(config);
    }
}
