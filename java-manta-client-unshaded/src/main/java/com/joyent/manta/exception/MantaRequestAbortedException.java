/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

/**
 * An exception used to indicate another exception was caused by a request being aborted.
 *
 */
public class MantaRequestAbortedException extends MantaIOException {

    /**
     * Wrap another exception in this class.
     *
     * @param e the cause
     */
    public MantaRequestAbortedException(Exception e) {
        super(e);
    }
}
