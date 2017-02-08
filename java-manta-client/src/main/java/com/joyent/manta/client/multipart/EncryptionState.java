/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client.multipart;

import com.joyent.manta.client.crypto.EncryptionContext;

import java.io.OutputStream;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Package private class that contains the state of the encryption streaming
 * ciphers used to encrypt multipart uploads.
 *
 * @author <a href="https://github.com/cburroughs/">Chris Burroughs</a>
 * @since 3.0.0
 */
class EncryptionState {
    /**
     * Encryption cipher state object.
     */
    private final EncryptionContext encryptionContext;

    /**
     * Lock used to coordinate concurrent operations.
     */
    private final ReentrantLock lock;

    /**
     * The number of the last part processed.
     */
    private int lastPartNumber = -1;

    /**
     * The multipart stream that allows for attaching and detaching streams.
     */
    private MultipartOutputStream multipartStream = null;

    /**
     * The encrypting stream.
     */
    private OutputStream cipherStream = null;

    /**
     * Creates a new multipart encryption state object.
     *
     * NOTE: This class is tightly bound to the lifestyle of the
     * encrypted MPU manager.  In particular the streams are now
     * instantiated as part of the constructor and must be set before
     * use.
     *
     * @param encryptionContext encryption cipher state object
     */
    EncryptionState(final EncryptionContext encryptionContext) {
        this.encryptionContext = encryptionContext;
        this.lock = new ReentrantLock();
    }

    EncryptionContext getEncryptionContext() {
        return encryptionContext;
    }

    ReentrantLock getLock() {
        return lock;
    }

    int getLastPartNumber() {
        return lastPartNumber;
    }

    MultipartOutputStream getMultipartStream() {
        return multipartStream;
    }

    OutputStream getCipherStream() {
        return cipherStream;
    }

    void setLastPartNumber(final int lastPartNumber) {
        this.lastPartNumber = lastPartNumber;
    }

    void setMultipartStream(final MultipartOutputStream multipartStream) {
        this.multipartStream = multipartStream;
    }

    void setCipherStream(final OutputStream cipherStream) {
        this.cipherStream = cipherStream;
    }
}
