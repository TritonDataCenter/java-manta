/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import java.io.OutputStream;
import java.util.Objects;
import java.util.UUID;
import javax.crypto.Cipher;

/**
 * This class holds references to clones of stateful objects used in streaming encryption operations.
 */
class EncryptionStateSnapshot {

    /**
     * Transaction ID for multipart upload.
     */
    private final UUID uploadId;

    /**
     * Part number at time of snapshot.
     */
    private final int lastPartNumber;

    /**
     * Cloned Cipher state.
     */
    private final Cipher cipher;

    /**
     * OutputStream duplicated at time of snapshot.
     */
    private final OutputStream outputStream;

    /**
     * EncryptionState's lastPartAuthWritten.
     */
    private final boolean lastPartAuthWritten;

    /**
     * @param uploadId       the {@link EncryptedMultipartUpload} transaction ID
     * @param lastPartNumber the lastPartNumber at the time of the snapshot
     * @param outputStream   the cloned {@link OutputStream}
     */
    EncryptionStateSnapshot(final UUID uploadId,
                            final int lastPartNumber,
                            final Cipher cipher,
                            final boolean lastPartAuthWritten,
                            final OutputStream outputStream) {
        this.uploadId = uploadId;
        this.lastPartNumber = lastPartNumber;
        this.cipher = cipher;
        this.outputStream = outputStream;
        this.lastPartAuthWritten = lastPartAuthWritten;
    }

    UUID getUploadId() {
        return uploadId;
    }

    int getLastPartNumber() {
        return lastPartNumber;
    }

    Cipher getCipher() {
        return cipher;
    }

    OutputStream getOutputStream() {
        return outputStream;
    }

    boolean getLastPartAuthWritten() {
        return lastPartAuthWritten;
    }

    @Override
    public int hashCode() {
        return Objects.hash(uploadId, lastPartNumber, outputStream);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final EncryptionStateSnapshot that = (EncryptionStateSnapshot) o;
        return uploadId == that.uploadId
                && lastPartNumber == that.lastPartNumber
                && outputStream == that.outputStream;
    }
}
