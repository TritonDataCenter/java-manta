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
 * Base implementation of a multipart upload state object.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public abstract class AbstractMultipartUpload implements MantaMultipartUpload {

    /**
     * Transaction ID for multipart upload.
     */
    private UUID id;

    /**
     * Path to final object being uploaded to Manta.
     */
    private String path;

    /**
     * Protected constructor for serialization purposes.
     */
    protected AbstractMultipartUpload() {
    }

    /**
     * Creates a new instance.
     *
     * @param uploadId Transaction ID for multipart upload
     * @param path Path to final object being uploaded to Manta
     */
    public AbstractMultipartUpload(final UUID uploadId, final String path) {
        this.id = uploadId;
        this.path = path;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final MantaMultipartUpload that = (MantaMultipartUpload) o;

        return Objects.equals(id, that.getId())
            && Objects.equals(path, that.getPath());
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, path);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("path", path)
                .toString();
    }

    @Override
    public int compare(final MantaMultipartUpload o1, final MantaMultipartUpload o2) {
        return o1.getPath().compareTo(o2.getPath());
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public String getPath() {
        return path;
    }
}
