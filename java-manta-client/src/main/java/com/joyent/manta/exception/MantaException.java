/*
 * Copyright (c) 2013-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

import com.joyent.manta.util.MantaVersion;
import org.apache.commons.lang3.exception.ContextedRuntimeException;

/**
 * The base MantaException class.
 * @author Yunong Xiao
 */
public class MantaException extends ContextedRuntimeException {

    private static final long serialVersionUID = 146894136987570504L;

    {
        addContextValue("mantaSdkVersion", MantaVersion.VERSION);
    }

    /**
     * Default constructor.
     */
    public MantaException() {
    }

    /**
     * @param message The exception message.
     */
    public MantaException(final String message) {
        super(message);
    }

    /**
     * @param cause The exception cause.
     */
    public MantaException(final Throwable cause) {
        super(cause);
    }

    /**
     * @param message The exception message.
     * @param cause The exception cause.
     */
    public MantaException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
