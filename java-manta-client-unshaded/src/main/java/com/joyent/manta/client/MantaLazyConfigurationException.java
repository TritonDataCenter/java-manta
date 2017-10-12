/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.exception.MantaException;

/**
 * Exception for errors occurring as a result of dynamic client reconfiguration.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celayac</a>
 * @since 3.1.7
 */
public class MantaLazyConfigurationException extends MantaException {

    private static final long serialVersionUID = 1560801372813448965L;

    /**
     * @param cause The exception cause.
     */
    public MantaLazyConfigurationException(final Throwable cause) {
        super(cause);
    }

    /**
     * @param message The exception message.
     * @param cause The exception cause.
     */
    public MantaLazyConfigurationException(final String message, final Exception cause) {
        super(message, cause);
    }
}
