/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http.entity;

import org.apache.http.Header;
import org.apache.http.HttpEntity;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Entity type used when not sending any content to the server.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public final class NoContentEntity implements HttpEntity {
    /**
     * Constant indicating an unknown content length.
     */
    private static final long NO_CONTENT_LENGTH = -1L;

    /**
     * Reusable instance of entity class.
     */
    public static final NoContentEntity INSTANCE = new NoContentEntity();

    /**
     * Private constructor because a single instance is all that we need.
     */
    private NoContentEntity() {

    }

    @Override
    public boolean isRepeatable() {
        return true;
    }

    @Override
    public boolean isChunked() {
        return true;
    }

    @Override
    public long getContentLength() {
        return NO_CONTENT_LENGTH;
    }

    @Override
    public Header getContentType() {
        return null;
    }

    @Override
    public Header getContentEncoding() {
        return null;
    }

    @Override
    public InputStream getContent() throws IOException, UnsupportedOperationException {
        return null;
    }

    @Override
    public void writeTo(final OutputStream outstream) throws IOException {
    }

    @Override
    public boolean isStreaming() {
        return false;
    }

    @Deprecated
    @Override
    public void consumeContent() throws IOException {
    }
}
