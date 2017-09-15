/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import com.joyent.manta.util.HmacOutputStream;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.commons.lang3.Validate;
import org.bouncycastle.crypto.macs.HMac;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.jcajce.io.CipherOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import javax.crypto.Cipher;

/**
 * Utility class that provides methods for cross-cutting encryption operations.
 *
 * @author <a href="https://github.com/cburroughs/">Chris Burroughs</a>
 * @since 3.0.0
 */
public final class EncryptingEntityHelper {
    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(EncryptingEntityHelper.class);

    /**
     * Private constructor for utility class.
     */
    private EncryptingEntityHelper() {
    }

    /**
     * Creates a new {@link OutputStream} implementation that is backed directly
     * by a {@link CipherOutputStream} or a {@link HmacOutputStream} that wraps
     * a {@link CipherOutputStream} depending on the encryption cipher/mode being
     * used. This allows us to support EtM authentication for ciphers/modes that
     * do not natively support authenticating encryption.
     *
     * NOTE: The design of com.joyent.manta.client.multipart.EncryptionStateRecorder
     * is heavily coupled to this implementation! Changing how these streams are
     * wrapped requires changes to EncryptionStateRecorder!
     *
     * @param httpOut           output stream for writing to the HTTP network socket
     * @param encryptionContext current encryption running state
     * @return a new stream configured based on the parameters
     */
    public static OutputStream makeCipherOutputForStream(
            final OutputStream httpOut, final EncryptionContext encryptionContext) {
        final HMac hmac;

        if (encryptionContext.getCipherDetails().isAEADCipher()) {
            hmac = null;
        } else {
            hmac = encryptionContext.getCipherDetails().getAuthenticationHmac();
            Validate.notNull(encryptionContext.getSecretKey(),
                    "Secret key must not be null");
            hmac.init(new KeyParameter(encryptionContext.getSecretKey().getEncoded()));
                /* The first bytes of the HMAC are the IV. This is done in order to
                 * prevent IV collision or spoofing attacks. */
            final byte[] iv = encryptionContext.getCipher().getIV();
            hmac.update(iv, 0, iv.length);
        }

        return makeCipherOutputForStream(
                httpOut,
                encryptionContext.getCipherDetails(),
                encryptionContext.getCipher(),
                hmac);
    }

    /**
     * Compatibility method for allowing older versions of java-manta-client-kryo-serialization to call this class.
     *
     * @param httpOut           output stream for writing to the HTTP network socket
     * @param encryptionContext current encryption running state
     * @param hmac              current HMAC object with the current checksum state
     * @return a new stream configured based on the parameters
     */
    public static OutputStream makeCipherOutputForStream(
            final OutputStream httpOut,
            final EncryptionContext encryptionContext,
            final HMac hmac) {
        return makeCipherOutputForStream(
                httpOut,
                encryptionContext.getCipherDetails(),
                encryptionContext.getCipher(),
                hmac);
    }

    /**
     * Creates a new {@link OutputStream} implementation that is backed directly
     * by a {@link CipherOutputStream} or a {@link HmacOutputStream} that wraps
     * a {@link CipherOutputStream} depending on the encryption cipher/mode being
     * used. This allows us to support EtM authentication for ciphers/modes that
     * do not natively support authenticating encryption.
     *
     * NOTE: The design of com.joyent.manta.client.multipart.EncryptionStateRecorder
     * is heavily coupled to this implementation! Changing how these streams are
     * wrapped requires changes to EncryptionStateRecorder!
     *
     * @param httpOut       output stream for writing to the HTTP network socket
     * @param cipherDetails information about the method of encryption in use
     * @param cipher        cipher to utilize for encrypting stream
     * @param hmac          current HMAC object with the current checksum state
     * @return a new stream configured based on the parameters
     */
    public static OutputStream makeCipherOutputForStream(
            final OutputStream httpOut,
            final SupportedCipherDetails cipherDetails,
            final Cipher cipher,
            final HMac hmac) {
        /* We have to use a "close shield" here because when close() is called
         * on a CipherOutputStream() for two reasons:
         *
         * 1. CipherOutputStream.close() writes additional bytes that a HMAC
         *    would need to read.
         * 2. Since we are going to append a HMAC to the end of the OutputStream
         *    httpOut, then we have to pretend to close it so that the HMAC bytes
         *    are not being written in the middle of the CipherOutputStream and
         *    thereby corrupting the ciphertext. */

        final CloseShieldOutputStream noCloseOut = new CloseShieldOutputStream(httpOut);
        final CipherOutputStream cipherOut = new CipherOutputStream(noCloseOut, cipher);
        final OutputStream out;

        Validate.notNull(cipherDetails,
                "Cipher details must not be null");
        Validate.notNull(cipher,
                "Cipher must not be null");

        // Things are a lot more simple if we are using AEAD
        if (cipherDetails.isAEADCipher()) {
            out = cipherOut;
        } else {
            out = new HmacOutputStream(hmac, cipherOut);
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Creating new OutputStream for multipart [{}]", out.getClass());
        }

        return out;
    }
}
