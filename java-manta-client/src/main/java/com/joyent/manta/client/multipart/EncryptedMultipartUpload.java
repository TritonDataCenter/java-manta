/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
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
    private final WRAPPED wrapped;

    /**
     * Object containing the state of encryption operations for the
     * current multipart upload.
     */
    private final EncryptionState encryptionState;

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
        return this.wrapped.getId();
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
