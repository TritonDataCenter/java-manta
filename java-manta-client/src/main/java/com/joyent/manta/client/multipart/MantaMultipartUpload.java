/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import java.util.Comparator;
import java.util.UUID;

/**
 * Interface representing a multipart upload in progress.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 2.5.0
 */
public interface MantaMultipartUpload extends Comparator<MantaMultipartUpload> {
    /**
     * @return upload id for the entire multipart upload operation
     */
    UUID getId();

    /**
     * @return path to the final object
     */
    String getPath();
}
