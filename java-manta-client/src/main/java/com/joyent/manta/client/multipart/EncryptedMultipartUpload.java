package com.joyent.manta.client.multipart;

import javax.crypto.Cipher;
import javax.crypto.Mac;
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
     * Wrapped inner implementation.
     */
    private final WRAPPED wrapped;

    /**
     * Cipher containing the current encryption state of all of the parts.
     */
    private final transient Cipher cipher;

    /**
     * HMAC instance that checksums non-AEAD ciphertext.
     */
    private final transient Mac hmac;

    /**
     * The counter for the parts processed. This is used to guarantee sequential
     * processing of parts.
     */
    private int currentPartNumber = 0;

    /**
     * Bytes from the last part that didn't properly fit into the cipher's
     * block size.
     */
    private byte[] lastBlockOverrunBytes;

    /**
     * Creates a new encrypted instance based on an existing instance.
     * @param wrapped instance to wrap
     */
    public EncryptedMultipartUpload(final WRAPPED wrapped) {
        this(wrapped, null, null);
    }

    /**
     * Creates a new encrypted instance based on an existing instance.
     * @param wrapped instance to wrap
     * @param cipher cipher instance used to encrypt the parts
     * @param hmac HMAC instance that checksums non-AEAD ciphertext
     */
    public EncryptedMultipartUpload(final WRAPPED wrapped,
                                    final Cipher cipher,
                                    final Mac hmac) {
        this.wrapped = wrapped;
        this.cipher = cipher;
        this.hmac = hmac;
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

    Cipher getCipher() {
        return cipher;
    }

    Mac getHmac() {
        return hmac;
    }

    void incrementPartNumber() {
        this.currentPartNumber++;
    }

    int getCurrentPartNumber() {
        return this.currentPartNumber;
    }

    byte[] getLastBlockOverrunBytes() {
        return this.lastBlockOverrunBytes;
    }

    void setLastBlockOverrunBytes(final byte[] lastBlockOverrunBytes) {
        this.lastBlockOverrunBytes = lastBlockOverrunBytes;
    }

    /**
     * Flag indicating that this object is in a state that it can be
     * used to track state for parts. When this returns false, this
     * object is used as an informational only object for the SDK
     * consumer.
     *
     * @return true when this object can track the state of uplaoded parts
     */
    boolean canUpload() {
        return this.cipher != null;
    }

    @Override
    public int compare(final MantaMultipartUpload o1, final MantaMultipartUpload o2) {
        return this.wrapped.compare(o1, o2);
    }
}
