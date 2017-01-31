package com.joyent.manta.client.crypto;

public class EncryptingEntityHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(EncryptingEntityHelper.class);


    public static CipherOutputStream makeCipherOutputforStream(final OuputStream out, final EncryptionContext eContext) {
        /* We have to use a "close shield" here because when close() is called
         * on a CipherOutputStream() for two reasons:
         *
         * 1. CipherOutputStream.close() writes additional bytes that a HMAC
         *    would need to read.
         * 2. Since we are going to append a HMAC to the end of the OutputStream
         *    httpOut, then we have to pretend to close it so that the HMAC bytes
         *    are not being written in the middle of the CipherOutputStream and
         *    thereby corrupting the ciphertext. */

        final CloseShieldOutputStream noCloseOut = new CloseShieldOutputStream(out);
        final CipherOutputStream cipherOut = new CipherOutputStream(noCloseOut, eContext.getCipher());
        final OutputStream out;
        final Mac hmac;

        // Things are a lot more simple if we are using AEAD
        if (eContext.getCipherDetails().isAEADCipher()) {
            out = cipherOut;
            hmac = null;
        } else {
            hmac = eContext.getCipherDetails().getAuthenticationHmac();
            try {
                hmac.init(eContext.getSecretKey());
                /* The first bytes of the HMAC are the IV. This is done in order to
                 * prevent IV collision or spoofing attacks. */
                hmac.update(eContext.getCipher().getIV());
            } catch (InvalidKeyException e) {
                String msg = "Error initializing HMAC with secret key";
                throw new MantaClientEncryptionException(msg, e);
            }
            out = new HmacOutputStream(hmac, cipherOut);
        }
        return out;

    }

    // /**
    //  * Copies the entity content to the specified output stream and validates
    //  * that the number of bytes copied is the same as specified when in the
    //  * original content-length.
    //  *
    //  * @param out stream to copy to
    //  * @throws IOException throw when there is a problem writing to the streams
    //  */
    // private void copyContentToOutputStream(final OutputStream out) throws IOException {
    //     final long bytesCopied;

    //     /* Only the EmbeddedHttpContent class requires us to actually call
    //      * write out on the wrapped object. In its particular case it is doing
    //      * a wrapping operation between an InputStream and an OutputStream in
    //      * order to provide an OutputStream interface to MantaClient. */
    //     if (this.wrapped.getClass().equals(EmbeddedHttpContent.class)) {
    //         CountingOutputStream cout = new CountingOutputStream(out);
    //         this.wrapped.writeTo(cout);
    //         cout.flush();
    //         bytesCopied = cout.getByteCount();
    //     } else {
    //         /* We choose a small buffer because large buffer don't result in
    //          * better performance when writing to a CipherOutputStream. You
    //          * can try this yourself by fiddling with this value and running
    //          * EncryptingEntityBenchmark. */
    //         final int bufferSize = 128;

    //         bytesCopied = IOUtils.copy(getContent(), out, bufferSize);
    //         out.flush();
    //     }

    //     /* If we don't know the length of the underlying content stream, we
    //      * count the number of bytes written, so that it is available. */
    //     if (originalLength == UNKNOWN_LENGTH) {
    //         originalLength = bytesCopied;
    //     } else if (originalLength != bytesCopied) {
    //         MantaIOException e = new MantaIOException("Bytes copied doesn't equal the "
    //                 + "specified content length");
    //         e.setContextValue("specifiedContentLength", originalLength);
    //         e.setContextValue("actualContentLength", bytesCopied);
    //         throw e;
    //     }
    // }
    

}
