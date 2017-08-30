/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.util.MantaUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;


/**
 * A single part of a multipart upload.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 2.5.0
 */
public class MantaMultipartUploadPart extends MantaMultipartUploadTuple
        implements Serializable {
    private static final long serialVersionUID = -738331736064518314L;

    /**
     * Remote path on Manta for the part's file.
     */
    private final String objectPath;

    /**
     * Creates a new instance based on explicitly defined parameters.
     *
     * @param partNumber Non-zero positive integer representing the relative position of the part
     * @param objectPath Remote path on Manta for the part's file
     * @param etag Etag value of the part
     */
    public MantaMultipartUploadPart(final int partNumber, final String objectPath,
                                    final String etag) {

        super(partNumber, etag);
        this.objectPath = objectPath;
    }

    /**
     * Creates a new instance based on a response from {@link MantaClient}.
     *
     * @param object response object from returned from {@link MantaClient}
     */
    public MantaMultipartUploadPart(final MantaObject object) {
        super(Integer.parseInt(MantaUtils.lastItemInPath(object.getPath())),
                object.getEtag());

        this.objectPath = object.getPath();
    }

    protected String getObjectPath() {
        return objectPath;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("partNumber", getPartNumber())
                .append("objectPath", getObjectPath())
                .append("etag", getEtag())
                .toString();
    }
}
