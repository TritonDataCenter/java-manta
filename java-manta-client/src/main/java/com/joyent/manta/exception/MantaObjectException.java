/*
 * Copyright (c) 2013-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

/**
 * @author Yunong Xiao
 */
public class MantaObjectException extends MantaException {

    private static final long serialVersionUID = 6799144229159150444L;

    /**
     * Default constructor.
     */
    public MantaObjectException() {
    }

    /**
     * @param message The exception message.
     */
    public MantaObjectException(final String message) {
        super(message);
    }

    /**
     * @param cause The exception cause.
     */
    public MantaObjectException(final Throwable cause) {
        super(cause);
    }

    /**
     * @param message The exception message.
     * @param cause The exception cause.
     */
    public MantaObjectException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
