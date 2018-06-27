/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import com.joyent.manta.http.HttpGetContinuator;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

import static java.util.Objects.requireNonNull;

/**
 * Stream which captures {@link IOException}s during read and swaps out the delegate stream to continue reading.
 * Effectively a {@link org.apache.commons.io.input.ProxyInputStream} but the error handling there is not useful to us
 * because an {@link InputStream} should block if no data is available. Unfortunately, {@code ProxyInputStream} returns
 * {@code EOF} after handling exceptions where instead we would rather loop again and attempt the read with a new
 * stream.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 * @since 3.2.3
 */
public class AutoContinuingInputStream extends InputStream {

    /**
     * The workhorse stream which reads from the actual source stream and keeps track of how many bytes it has read.
     */
    private final ContinuingInputStream in;

    /**
     * Produces continuations of the original stream given new byte offsets.
     */
    private final HttpGetContinuator continuator;

    /**
     * Construct a resilient {@code InputStream} with a reusable stream and a helper that can supply "suffixes"
     * (generally referred to as "continuations") of the original stream given a new starting offset.
     *
     * @param in the stream to which we delegate reads
     * @param continuator helper that can produce continuations of {@code in}
     */
    public AutoContinuingInputStream(final ContinuingInputStream in,
                                     final HttpGetContinuator continuator) {
        this.in = requireNonNull(in);
        this.continuator = requireNonNull(continuator);
    }

    /**
     * Attempts to build a continuation of the stream we are trying to read if our {@link #continuator} considers the
     * exception non-fatal, passing along the byte offset from which the continuation should pick up.
     *
     * @param e the exception from which we are attempting to recover
     * @throws IOException the exception if it is not recoverable, or an exception that may have occurred while
     * continuing
     */
    private void attemptRecovery(final IOException e) throws IOException {
        this.in.continueWith(this.continuator.buildContinuation(e, this.in.getBytesRead()));
    }

    @Override
    public int read() throws IOException {
        while (true) {
            try {
                return this.in.read();
            } catch (final IOException e) {
                this.attemptRecovery(e);
            }
        }
    }

    @Override
    public int read(final byte[] b) throws IOException {
        while (true) {
            try {
                return this.in.read(b);
            } catch (final IOException e) {
                this.attemptRecovery(e);
            }
        }
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        while (true) {
            try {
                return this.in.read(b, off, len);
            } catch (final IOException e) {
                this.attemptRecovery(e);
            }
        }
    }

    @Override
    public long skip(final long ln) throws IOException {
        while (true) {
            try {
                return this.in.skip(ln);
            } catch (final IOException e) {
                this.attemptRecovery(e);
            }
        }
    }

    @Override
    public int available() throws IOException {
        while (true) {
            try {
                return this.in.available();
            } catch (final IOException e) {
                this.attemptRecovery(e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public void mark(final int readlimit) {
        throw new UncheckedIOException(new IOException("mark/reset not supported"));
    }

    @Override
    public void reset() throws IOException {
        throw new IOException("mark/reset not supported");
    }

    @Override
    public boolean markSupported() {
        return false;
    }

}
