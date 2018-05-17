/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.net.ssl.SSLException;

/**
 * Class.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 */
public class RecoverableDownloadMantaIOException extends MantaIOException {

    private static final long serialVersionUID = -6231685244423701024L;

    private static final Set<Class<? extends IOException>> EXCEPTIONS_FATAL =
            Collections.unmodifiableSet(
                    new HashSet<>(
                            Arrays.asList(
                                    InterruptedIOException.class,
                                    UnknownHostException.class,
                                    ConnectException.class,
                                    SSLException.class)));

    /**
     * words.
     *
     * @param cause cause
     */
    public RecoverableDownloadMantaIOException(final IOException cause) {
        super(cause);
    }

    /**
     * more words.
     *
     * @param cause   cause
     * @param message message
     */
    public RecoverableDownloadMantaIOException(final IOException cause,
                                               final String message) {
        super(message, cause);
    }


    public static boolean isResumableError(final IOException e) {
        if (EXCEPTIONS_FATAL.contains(e.getClass())) {
            return false;
        }

        for (final Class<? extends IOException> exceptionClass : EXCEPTIONS_FATAL) {
            if (exceptionClass.isInstance(e)) {
                return false;
            }
        }

        return true;
    }
}
