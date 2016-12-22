/**
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.util;

import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.util.Arrays;

/**
 * {@link OutputStream} implementation that doesn't write any data anywhere, but instead
 * updates N digests for every write() invocation.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class DigestNoopOutputStream extends OutputStream {
    /**
     * Array of digests to update on writes.
     */
    private final MessageDigest[] digests;

    /**
     * Creates a new instance initialized with the digests to update.
     * @param digests digest objects to update on write()
     */
    public DigestNoopOutputStream(final MessageDigest[] digests) {
        Validate.notEmpty(digests, "At least one digest must be specified");
        this.digests = digests;
    }

    /**
     * Returns the message digest(s) associated with this stream.
     *
     * @return the message digest associated with this stream
     */
    public MessageDigest[] getDigests() {
        return digests;
    }

    @Override
    public void write(final int b) throws IOException {
        for (MessageDigest d : digests) {
            d.update((byte)b);
        }
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        for (MessageDigest d : digests) {
            d.update(b, off, len);
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final DigestNoopOutputStream that = (DigestNoopOutputStream) o;

        return Arrays.equals(digests, that.digests);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digests);
    }

    @Override
    public String toString() {
        final ToStringBuilder toStringBuilder = new ToStringBuilder(this);
        for (MessageDigest d : digests) {
            toStringBuilder.append(d);
        }
        return toStringBuilder.toString();
    }
}