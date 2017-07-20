/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import java.util.Objects;
import java.util.UUID;

/**
 * Multipart upload state object used with client-side encryption.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 *
 * @param <WRAPPED> Multipart upload class type to wrap with encryption
 */
public class EncryptedMultipartUpload<WRAPPED extends MantaMultipartUpload>
        implements MantaMultipartUpload {
    /**
     * Backing implementation used by the underlying multipart implementation.
     */
    private WRAPPED wrapped;

    /**
     * Object containing the state of encryption operations for the
     * current multipart upload.
     */
    private EncryptionState encryptionState;

    /**
     * Private constructor used for serialization.
     */
    private EncryptedMultipartUpload() {
    }

    /**
     * Creates a new encrypted instance based on an existing instance.
     * @param wrapped instance to wrap
     */
    public EncryptedMultipartUpload(final WRAPPED wrapped) {
        this(wrapped, null);
    }

    /**
     * Creates a new encrypted instance based on an existing instance.
     * @param wrapped instance to wrap
     * @param encryptionState state of encryption operations for the MPU
     */
    public EncryptedMultipartUpload(final WRAPPED wrapped,
                                    final EncryptionState encryptionState) {
        this.wrapped = wrapped;
        this.encryptionState = encryptionState;
    }

    @Override
    public UUID getId() {
        if (this.wrapped != null) {
            return this.wrapped.getId();
        }

        return null;
    }

    @Override
    public String getPath() {
        return this.wrapped.getPath();
    }

    /**
     * @return the backing instance
     */
    WRAPPED getWrapped() {
        return wrapped;
    }

    EncryptionState getEncryptionState() {
        return encryptionState;
    }

    /**
     * Flag indicating that this object is in a state that it can be
     * used to track state for parts. When this returns false, this
     * object is used as an informational only object for the SDK
     * consumer.
     *
     * @return true when this object can track the state of uploaded parts
     */
    boolean canUpload() {
        return this.encryptionState != null;
    }

    @Override
    public int compare(final MantaMultipartUpload o1, final MantaMultipartUpload o2) {
        return this.wrapped.compare(o1, o2);
    }

    @Override
    public String toString() {
        return wrapped.toString();
    }

    /**
     * NOTE: Upload object equality is based on the underlying wrapped
     * object and not the state of any ciphers, streams, or locks.
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final EncryptedMultipartUpload<?> that = (EncryptedMultipartUpload<?>) o;

        return Objects.equals(wrapped, that.wrapped);
    }

    @Override
    public int hashCode() {
        return Objects.hash(wrapped);
    }
}
