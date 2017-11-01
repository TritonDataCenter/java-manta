/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.http.MantaHttpHeaders;

import java.io.IOException;

import static com.joyent.manta.util.MantaUtils.writeablePrefixPaths;

/**
 * Directory creation strategy which creates all non-system directories every time. Used if the depth skipping probe
 * fails. This is the original behavior of recursive putDirectory.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celayac</a>
 * @since 3.1.7
 */
class PessimisticDirectoryCreationStrategy extends RecursiveDirectoryCreationStrategy {

    PessimisticDirectoryCreationStrategy(final MantaClient client) {
        super(client);
    }

    @Override
    public void create(final String rawPath, final MantaHttpHeaders headers) throws IOException {
        for (final String path : writeablePrefixPaths(rawPath)) {
            getClient().putDirectory(path, headers);
            incrementOperations();
        }
    }
}
