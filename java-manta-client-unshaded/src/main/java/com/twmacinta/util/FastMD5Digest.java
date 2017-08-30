/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.twmacinta.util;

import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.EncodableDigest;
import org.bouncycastle.util.Memoable;

import java.nio.ByteBuffer;

/**
 * BouncyCastle {@link Digest} implementation of {@link MD5} (FastMD5).
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class FastMD5Digest implements Digest, EncodableDigest, Memoable {
    /**
     * Digest size in bytes.
     */
    private static final int DIGEST_LENGTH = 16;

    /**
     * Backing instance.
     */
    private final MD5 md5;

    /**
     * Creates a new instance.
     */
    public FastMD5Digest() {
        this.md5 = new MD5();
    }

    /**
     * Creates an instance backed by the specified state.
     *
     * @param encodedState state represented as a byte array
     */
    public FastMD5Digest(final byte[] encodedState) {
        this.md5 = new MD5();
        updateStateFromEncodedState(this.md5.state, encodedState);
    }

    /**
     * Creates an instance backed by the specified state.
     *
     * @param state state as a state object
     */
    private FastMD5Digest(final MD5State state) {
        this.md5 = new MD5();
        this.md5.state = state;
    }

    @Override
    public String getAlgorithmName() {
        return "MD5";
    }

    @Override
    public int getDigestSize() {
        return DIGEST_LENGTH;
    }

    @Override
    public void update(final byte in) {
        md5.Update(in);
    }

    @Override
    public void update(final byte[] in, final int inOff, final int len) {
        md5.Update(in, inOff, len);
    }

    @Override
    public int doFinal(final byte[] out, final int outOff) {
        final byte[] finalValue = md5.Final();

        System.arraycopy(finalValue, 0, out, outOff, finalValue.length);

        return DIGEST_LENGTH;
    }

    @Override
    public void reset() {
        md5.Init();
    }

    @Override
    public byte[] getEncodedState() {
        final MD5State state = md5.state;
        return generateEncodedState(state);
    }

    @Override
    public Memoable copy() {
        final MD5State state = new MD5State(this.md5.state);
        return new FastMD5Digest(state);
    }

    @Override
    public void reset(final Memoable other) {
        if (!other.getClass().equals(FastMD5Digest.class)) {
            throw new UnsupportedOperationException("Reset is only supported "
                + "from another FastMD5Digest class");
        }

        FastMD5Digest d = (FastMD5Digest)other;
        this.md5.state = new MD5State(d.md5.state);
    }

    /**
     * Extracts the running state from a {@link MD5State} object and encodes
     * it as a byte array.
     *
     * @param md5State state object to process
     * @return byte array encoded with the state
     */
    static byte[] generateEncodedState(final MD5State md5State) {
        final int stateBufferSize =
                md5State.buffer.length
                        + Integer.BYTES
                        + (md5State.state.length * Integer.BYTES)
                        + Integer.BYTES
                        + Long.BYTES;
        final ByteBuffer bytes = ByteBuffer.allocate(stateBufferSize);

        bytes.putInt(md5State.buffer.length);
        bytes.put(md5State.buffer);
        bytes.putInt(md5State.state.length);

        for (int i : md5State.state) {
            bytes.putInt(i);
        }

        bytes.putLong(md5State.count);

        return bytes.array();
    }

    /**
     * Copies the state from an encoded byte array to a state object.
     *
     * @param md5State state object to copy to
     * @param encodedState encoded state as byte array
     */
    static void updateStateFromEncodedState(final MD5State md5State,
                                            final byte[] encodedState) {
        final ByteBuffer bytes = ByteBuffer.wrap(encodedState);

        final byte[] buffer = new byte[bytes.getInt()];
        bytes.get(buffer);
        final int[] state = new int[bytes.getInt()];

        for (int i = 0; i < state.length; i++) {
            state[i] = bytes.getInt();
        }

        final long count = bytes.getLong();

        md5State.buffer = buffer;
        md5State.state = state;
        md5State.count = count;
    }
}
