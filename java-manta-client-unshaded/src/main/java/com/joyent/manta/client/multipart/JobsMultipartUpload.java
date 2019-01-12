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
 *
 * @deprecated This class will be removed in the next major version. MPU
 * functionality is now available server-side and no longer needs to be done
 * using jobs.
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 2.5.0
 */
@Deprecated
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
