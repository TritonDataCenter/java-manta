package com.joyent.manta.client.crypto;

import com.joyent.manta.exception.MantaClientEncryptionException;
import com.joyent.manta.util.HmacOutputStream;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.CipherOutputStream;
import javax.crypto.Mac;
import java.io.OutputStream;
import java.security.InvalidKeyException;

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
     * @param httpOut output stream for writing to the HTTP network socket
     * @param encryptionContext current encryption running state
     * @return a new stream configured based on the parameters
     */
    public static OutputStream makeCipherOutputForStream(
            final OutputStream httpOut, final EncryptionContext encryptionContext) {
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
        final CipherOutputStream cipherOut = new CipherOutputStream(noCloseOut, encryptionContext.getCipher());
        final OutputStream out;
        final Mac hmac;

        // Things are a lot more simple if we are using AEAD
        if (encryptionContext.getCipherDetails().isAEADCipher()) {
            out = cipherOut;
        } else {
            hmac = encryptionContext.getCipherDetails().getAuthenticationHmac();
            try {
                hmac.init(encryptionContext.getSecretKey());
                /* The first bytes of the HMAC are the IV. This is done in order to
                 * prevent IV collision or spoofing attacks. */
                hmac.update(encryptionContext.getCipher().getIV());
            } catch (InvalidKeyException e) {
                String msg = "Error initializing HMAC with secret key";
                throw new MantaClientEncryptionException(msg, e);
            }
            out = new HmacOutputStream(hmac, cipherOut);
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Creating new OutputStream for multipart [{}]", out.getClass());
        }

        return out;
    }
}
