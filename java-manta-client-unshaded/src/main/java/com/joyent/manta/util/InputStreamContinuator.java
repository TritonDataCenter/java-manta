/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

/**
 * "Factory" class for resuming the body of a response being read as an {@link InputStream} based on the number of bytes
 * already read. Meant to be used in combination with {@link com.joyent.manta.util.ContinuingInputStream}. The
 * {@link Closeable} is included so that the number of continuations provided by a single continuator can be
 * instrumented.
 *
 * Could also be called "InputStreamFactory" or "InputStreamContinuationFactory" but those are a bit too ambiguous.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 * @since 3.2.3
 */
public interface InputStreamContinuator extends Closeable {

    /**
     * Get an {@link InputStream} which picks up starting {@code bytesRead} bytes from the beginning of the logical
     * object being downloaded. Implementations should compare headers across all requests and responses to ensure
     * that the object being downloaded has not changed between the initial and subsequent requests.
     *
     * @param ex the exception which occurred while downloading (either the first response or a continuation)
     * @param bytesRead byte offset at which the new stream should start
     * @return another stream which continues to deliver the bytes from the initial request
     * @throws IOException if the exception is not recoverable or there is an error preparing the continuation
     */
    InputStream buildContinuation(IOException ex, long bytesRead) throws IOException;
}
