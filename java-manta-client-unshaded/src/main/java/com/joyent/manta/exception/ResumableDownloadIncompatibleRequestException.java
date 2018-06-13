/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

public class ResumableDownloadIncompatibleRequestException extends ResumableDownloadException {

    private static final long serialVersionUID = 7415723473743850334L;

    /**
     * {@inheritDoc}
     */
    public ResumableDownloadIncompatibleRequestException(final String msg) {
        super(msg);
    }

    /**
     * {@inheritDoc}
     */
    public ResumableDownloadIncompatibleRequestException(final Throwable cause) {
        super(cause);
    }

    /**
     * {@inheritDoc}
     */
    public ResumableDownloadIncompatibleRequestException(final String msg, final Throwable cause) {
        super(msg, cause);
    }
}
