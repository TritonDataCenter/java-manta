/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

import org.apache.http.HttpResponse;

import java.util.Arrays;

import static org.apache.http.HttpHeaders.CONTENT_RANGE;
import static org.apache.http.HttpHeaders.ETAG;

/**
 * Exception signaling that a resumed download request cannot be continued because the response was invalid.
 * TODO: Do we actually need a new exception?
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 * @since 3.2.2
 */
public class ResumableDownloadUnexpectedResponseException extends ResumableDownloadException {

    private static final long serialVersionUID = 123778476650917899L;

    /**
     * {@inheritDoc}
     */
    public ResumableDownloadUnexpectedResponseException(final String msg) {
        super(msg);
    }

    /**
     * {@inheritDoc}
     */
    public ResumableDownloadUnexpectedResponseException(final Throwable cause) {
        super(cause);
    }

    /**
     * {@inheritDoc}
     */
    public ResumableDownloadUnexpectedResponseException(final String msg, final Throwable cause) {
        super(msg, cause);
    }

    public ResumableDownloadUnexpectedResponseException(final String msg, final HttpResponse response) {
        super(msg);
        this.setContextValue("responseHeader_etag", Arrays.deepToString(response.getHeaders(ETAG)));
        this.setContextValue("responseHeader_content-range", Arrays.deepToString(response.getHeaders(CONTENT_RANGE)));
    }
}
