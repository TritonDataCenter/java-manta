/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

/**
 * A value only class used to store the state of various numeric values
 * that are the result of converting between a plaintext byte range and
 * a ciphertext byte range.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class ByteRangeConversion {
    /**
     * The ciphertext starting byte to read from.
     */
    private final long ciphertextStartPositionInclusive;

    /**
     * The number of plaintext bytes to skip after we start reading ciphertext.
     * In many cipher implementations, ciphertext can only be read in chunks of
     * blocks, so we have to start reading at a block and then skip plaintext
     * bytes that are read from the block so that we can seek to a given plaintext
     * position.
     */
    private final long plaintextBytesToSkipInitially;

    /**
     * The ciphertext ending byte to read until.
     */
    private final long ciphertextEndPositionInclusive;

    /**
     * The total length of the plaintext to be read including the value
     * of plaintextBytesToSkipInitially.
     */
    private final long lengthOfPlaintextIncludingSkipBytes;

    /**
     * If this is the result of a block by block conversation, this
     * is the number of the first block in which the start position resides.
     */
    private final long startingBlockNumberInclusive;

    /**
     * Creates a new instance of a range conversion value object.
     *
     * @param ciphertextStartPositionInclusive the ciphertext starting byte to read from
     * @param plaintextBytesToSkipInitially the number of plaintext bytes to skip after we start reading ciphertext
     * @param ciphertextEndPositionInclusive the ciphertext ending byte to read until
     * @param lengthOfPlaintextIncludingSkipBytes the total length of the plaintext to be read
     * @param startingBlockNumberInclusive the number of the first block in which the start position resides
     */
    public ByteRangeConversion(final long ciphertextStartPositionInclusive,
                               final long plaintextBytesToSkipInitially,
                               final long ciphertextEndPositionInclusive,
                               final long lengthOfPlaintextIncludingSkipBytes,
                               final long startingBlockNumberInclusive) {
        this.ciphertextStartPositionInclusive = ciphertextStartPositionInclusive;
        this.plaintextBytesToSkipInitially = plaintextBytesToSkipInitially;
        this.ciphertextEndPositionInclusive = ciphertextEndPositionInclusive;
        this.lengthOfPlaintextIncludingSkipBytes = lengthOfPlaintextIncludingSkipBytes;
        this.startingBlockNumberInclusive = startingBlockNumberInclusive;
    }

    public long getCiphertextStartPositionInclusive() {
        return ciphertextStartPositionInclusive;
    }

    public long getPlaintextBytesToSkipInitially() {
        return plaintextBytesToSkipInitially;
    }

    public long getCiphertextEndPositionInclusive() {
        return ciphertextEndPositionInclusive;
    }

    public long getLengthOfPlaintextIncludingSkipBytes() {
        return lengthOfPlaintextIncludingSkipBytes;
    }

    public long getStartingBlockNumberInclusive() {
        return startingBlockNumberInclusive;
    }
}
