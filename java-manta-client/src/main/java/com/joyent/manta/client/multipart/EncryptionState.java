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
    // FIXME with public stuff
    public final EncryptionContext eContext;
    public final ReentrantLock lock;
    public int lastPartNumber = -1;
    public MultipartOutputStream multipartStream = null;
    public OutputStream cipherStream = null;

    //cipher or entity junk? Context?
    // iostream
    // lock!?!
    public EncryptionState(final EncryptionContext encryptionContext) {
        this.eContext = encryptionContext;
        this.lock = new ReentrantLock();
    }
}
