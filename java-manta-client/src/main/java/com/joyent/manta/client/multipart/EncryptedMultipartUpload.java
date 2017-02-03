package com.joyent.manta.client.multipart;

import java.util.UUID;

/**
 * Multipart upload state object used with client-side encryption.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class EncryptedMultipartUpload<WRAPPED extends MantaMultipartUpload>
        implements MantaMultipartUpload {
    private static final long serialVersionUID = -4122727303942324688L;

    /**
     * Wrapped inner implementation.
     */
    private final WRAPPED wrapped;

    /**
     * Creates a new encrypted instance based on an existing instance.
     * @param wrapped instance to wrap
     */
    public EncryptedMultipartUpload(final WRAPPED wrapped) {
        this.wrapped = wrapped;
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
    public WRAPPED getWrapped() {
        return wrapped;
    }

    @Override
    public int compare(final MantaMultipartUpload o1, final MantaMultipartUpload o2) {
        return this.wrapped.compare(o1, o2);
    }
}
