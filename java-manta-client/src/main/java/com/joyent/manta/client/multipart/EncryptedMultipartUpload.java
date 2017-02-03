package com.joyent.manta.client.multipart;

import javax.crypto.Cipher;
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
    private static final long serialVersionUID = -4122727303942324688L;

    /**
     * Wrapped inner implementation.
     */
    private final WRAPPED wrapped;

    /**
     * Cipher containing the current encryption state of all of the parts.
     */
    private final Cipher cipher;

    /**
     * The counter for the parts processed. This is used to guarantee sequential
     * processing of parts.
     */
    private int currentPartNumber = 0;

    /**
     * Creates a new encrypted instance based on an existing instance.
     * @param wrapped instance to wrap
     * @param cipher cipher instance used to encrypt the parts
     */
    public EncryptedMultipartUpload(final WRAPPED wrapped, final Cipher cipher) {
        this.wrapped = wrapped;
        this.cipher = cipher;
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

    public Cipher getCipher() {
        return cipher;
    }

    @Override
    public int compare(final MantaMultipartUpload o1, final MantaMultipartUpload o2) {
        return this.wrapped.compare(o1, o2);
    }
}
