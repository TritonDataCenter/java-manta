/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http.entity;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;

import java.io.InputStream;

/**
 * A Manta-specific implementation of {@link org.apache.http.entity.InputStreamEntity}.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class MantaInputStreamEntity extends InputStreamEntity {
    /**
     * Creates an entity with an unknown length.
     * Equivalent to {@code new InputStreamEntity(instream, -1)}.
     *
     * @param instream input stream
     * @throws IllegalArgumentException if {@code instream} is {@code null}
     * @since 4.3
     */
    public MantaInputStreamEntity(final InputStream instream) {
        super(instream);
    }

    /**
     * Creates an entity with a specified content length.
     *
     * @param instream input stream
     * @param length   of the input stream, {@code -1} if unknown
     * @throws IllegalArgumentException if {@code instream} is {@code null}
     */
    public MantaInputStreamEntity(final InputStream instream, final long length) {
        super(instream, length);
    }

    /**
     * Creates an entity with a content type and unknown length.
     * Equivalent to {@code new InputStreamEntity(instream, -1, contentType)}.
     *
     * @param instream    input stream
     * @param contentType content type
     * @throws IllegalArgumentException if {@code instream} is {@code null}
     * @since 4.3
     */
    public MantaInputStreamEntity(final InputStream instream, final ContentType contentType) {
        super(instream, contentType);
    }

    /**
     * @param instream    input stream
     * @param length      of the input stream, {@code -1} if unknown
     * @param contentType for specifying the {@code Content-Type} header, may be {@code null}
     * @throws IllegalArgumentException if {@code instream} is {@code null}
     * @since 4.2
     */
    public MantaInputStreamEntity(final InputStream instream, final long length,
                                  final ContentType contentType) {
        super(instream, length, contentType);
    }
}

