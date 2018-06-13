/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

public class ResumableDownloadException extends MantaIOException {
    private static final long serialVersionUID = -5972256969855482635L;

    /**
     * {@inheritDoc}
     */
    public ResumableDownloadException(final String msg) {
        super(msg);
    }

    /**
     * {@inheritDoc}
     */
    public ResumableDownloadException(final Throwable cause) {
        super(cause);
    }

    /**
     * {@inheritDoc}
     */
    public ResumableDownloadException(final String msg, final Throwable cause) {
        super(msg, cause);
    }
}
