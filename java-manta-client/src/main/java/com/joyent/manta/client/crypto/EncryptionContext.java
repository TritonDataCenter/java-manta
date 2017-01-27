package com.joyent.manta.client.crypto;

import com.joyent.manta.exception.MantaClientEncryptionException;

import javax.crypto.Cipher;
import java.security.InvalidKeyException;
import java.security.InvalidAlgorithmParameterException;
import javax.crypto.SecretKey;


// context around a single encryption operation.  namely the secret + cipher info, with an inited IV
public class EncryptionContext {
    /**
     * Secret key to encrypt stream with.
     */
    private final SecretKey key;

    /**
     * Attributes of the cipher used for encryption.
     */
    private final SupportedCipherDetails cipherDetails;

    /**
     * Cipher implementation used to encrypt as a stream.
     */
    private final Cipher cipher;


    public EncryptionContext(final SecretKey key,
                             final SupportedCipherDetails cipherDetails) {
        @SuppressWarnings("MagicNumber")
        final int keyBits = key.getEncoded().length << 3;

        if (keyBits != cipherDetails.getKeyLengthBits()) {
            String msg = "Mismatch between algorithm definition and secret key size";
            MantaClientEncryptionException e = new MantaClientEncryptionException(msg);
            e.setContextValue("cipherDetails", cipherDetails.toString());
            e.setContextValue("secretKeyAlgorithm", key.getAlgorithm());
            e.setContextValue("secretKeySizeInBits", String.valueOf(keyBits));
            e.setContextValue("expectedKeySizeInBits", cipherDetails.getKeyLengthBits());
            throw e;
        }

        this.key = key;
        this.cipherDetails = cipherDetails;
        this.cipher = cipherDetails.getCipher();
        initializeCipher();
    }


    public SecretKey getSecretKey() {
        return key;
    }

    public SupportedCipherDetails getCipherDetails() {
        return cipherDetails;
    }
    
    public Cipher getCipher() {
        return cipher;
    }



    /**
     * Initializes the cipher with an IV (initialization vector), so that
     * the cipher is ready to be used to encrypt.
     */
    private void initializeCipher() {
        try {
            byte[] iv = cipherDetails.generateIv();
            cipher.init(Cipher.ENCRYPT_MODE, this.key, cipherDetails.getEncryptionParameterSpec(iv));
        } catch (InvalidKeyException e) {
            MantaClientEncryptionException mcee = new MantaClientEncryptionException(
                    "There was a problem loading private key", e);
            String details = String.format("key=%s, algorithm=%s",
                    key.getAlgorithm(), key.getFormat());
            mcee.setContextValue("key_details", details);
            throw mcee;
        } catch (InvalidAlgorithmParameterException e) {
            throw new MantaClientEncryptionException(
                    "There was a problem with the passed algorithm parameters", e);
        }
    }
    
}
