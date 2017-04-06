/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Objects;
import java.util.UUID;

/**
 * Multipart upload state object used for server-side MPU.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class ServerSideMultipartUpload extends AbstractMultipartUpload {
    /**
     * Directory containing the multipart upload parts.
     */
    private String partsDirectory;

    /**
     * Private constructor for serialization purposes.
     */
    private ServerSideMultipartUpload() {
        super();
    }

    /**
     * Creates a new instance with a parts directory in addition to the
     * parameters used in the super class.
     *
     * @param uploadId Transaction ID for multipart upload
     * @param path Path to final object being uploaded to Manta
     * @param partsDirectory path to the multipart parts directory on manta for this upload
     */
    public ServerSideMultipartUpload(final UUID uploadId, final String path,
                                     final String partsDirectory) {
        super(uploadId, path);
        this.partsDirectory = partsDirectory;
    }

    public String getPartsDirectory() {
        return partsDirectory;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        ServerSideMultipartUpload that = (ServerSideMultipartUpload)o;

        return Objects.equals(partsDirectory, that.partsDirectory);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), partsDirectory);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
            .append("id", getId())
            .append("path", getPath())
            .append("partsDirectory", partsDirectory)
            .toString();
    }
}
