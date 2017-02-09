/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import com.joyent.manta.client.MantaClient;

/**
 * Convenience class that implements the generic interface for an encrypted
 * server side multipart manager and creates the underlying wrapped instance
 * so that the API consumer doesn't have to wrangle with so many generics.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class EncryptedServerSideMultipartManager
        extends EncryptedMultipartManager<ServerSideMultipartManager, ServerSideMultipartUpload> {
    /**
     * Creates a new encrypted multipart upload manager that is backed by
     * a server-side supported manager.
     *
     * @param mantaClient manta client use for MPU operations
     */
    public EncryptedServerSideMultipartManager(final MantaClient mantaClient) {
        super(mantaClient, new ServerSideMultipartManager(mantaClient));
    }
}
