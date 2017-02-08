/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import java.util.UUID;

/**
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 2.5.0
 */
public class JobsMultipartUpload extends AbstractMultipartUpload {
    /**
     * Creates a new instance associated with the specified id
     * and object path.
     *
     * @param uploadId Transaction ID for multipart upload
     * @param path Path to final object being uploaded to Manta
     */
    public JobsMultipartUpload(final UUID uploadId, final String path) {
        super(uploadId, path);
    }
}
