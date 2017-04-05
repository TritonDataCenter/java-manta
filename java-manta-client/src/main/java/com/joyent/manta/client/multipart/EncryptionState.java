/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import com.joyent.manta.client.crypto.EncryptionContext;
import com.joyent.manta.exception.MantaMultipartException;
import com.joyent.manta.util.HmacOutputStream;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.Validate;
import org.bouncycastle.crypto.macs.HMac;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Package private class that contains the state of the encryption streaming
 * ciphers used to encrypt multipart uploads.
 *
 * @author <a href="https://github.com/cburroughs/">Chris Burroughs</a>
 * @since 3.0.0
 */
public class EncryptionState {

    /**
     * Logger instance.
     */
    private static final transient Logger LOGGER = LoggerFactory.getLogger(EncryptionState.class);

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
    private transient MultipartOutputStream multipartStream = null;

    /**
     * The encrypting stream.
     */
    private transient OutputStream cipherStream = null;

    /**
     * Indicates if the multipart stream buffer has been flushed and
     * final cipher auth bytes written.
     */
    private boolean lastPartAuthWritten = false;

    /**
     * Zero argument constructor used for serialization.
     */
    private EncryptionState() {
        this.encryptionContext = null;
        this.lock = new ReentrantLock();
    }

    /**
     * <p>Creates a new multipart encryption state object.</p>
     *
     * <p>NOTE: This class is tightly bound to the lifecycle of the
     * encrypted MPU manager.  In particular the streams are now
     * instantiated as part of the constructor and must be set before
     * use.</p>
     *
     * @param encryptionContext encryption cipher state object
     */
    public EncryptionState(final EncryptionContext encryptionContext) {
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

    boolean isLastPartAuthWritten() {
        return lastPartAuthWritten;
    }

    ByteArrayOutputStream remainderAndLastPartAuth() throws IOException {
        if (!getLock().isHeldByCurrentThread()) {
            throw new IllegalStateException("remainderAndLastPartAuth called without lock owned");
        }
        if (isLastPartAuthWritten()) {
            final String msg = "final CSE auth already written (complete called multiple times or "
                + "parts below min size)";
            throw new MantaMultipartException(new IllegalStateException(msg));
        }
        ByteArrayOutputStream remainderStream = new ByteArrayOutputStream();
        getMultipartStream().setNext(remainderStream);
        getCipherStream().close();
        remainderStream.write(getMultipartStream().getRemainder());

        if (getCipherStream().getClass().equals(HmacOutputStream.class)) {
            HMac hmac = ((HmacOutputStream) getCipherStream()).getHmac();
            byte[] hmacBytes = new byte[hmac.getMacSize()];
            hmac.doFinal(hmacBytes, 0);

            final int hmacSize = encryptionContext.getCipherDetails().getAuthenticationTagOrHmacLengthInBytes();

            Validate.isTrue(hmacBytes.length == hmacSize,
                            "HMAC actual bytes doesn't equal the number of bytes expected");

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("HMAC: {}", Hex.encodeHexString(hmacBytes));
            }
            remainderStream.write(hmacBytes);
        }
        lastPartAuthWritten = true;
        return remainderStream;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final EncryptionState that = (EncryptionState) o;

        return lastPartNumber == that.lastPartNumber
            && lastPartAuthWritten == that.lastPartAuthWritten
            && Objects.equals(encryptionContext, that.encryptionContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(encryptionContext, lastPartNumber);
    }
}
