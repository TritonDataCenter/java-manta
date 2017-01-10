package com.joyent.manta.http;

import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.config.EncryptionObjectAuthenticationMode;

import javax.crypto.SecretKey;

/**
 * {@link HttpHelper} implementation that transparently handles client-side
 * encryption when using the Manta server API.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class EncryptionHttpHelper extends StandardHttpHelper {
    /**
     * The unique identifier of the key used for encryption.
     */
    private final String encryptionKeyId;

    /**
     * True when downloading unencrypted files is allowed in encryption mode.
     */
    private final boolean permitUnencryptedDownloads;

    /**
     * Specifies if we are in strict ciphertext authentication mode or not.
     */
    private final EncryptionObjectAuthenticationMode encryptionAuthenticationMode;

    /**
     * Secret key used to encrypt and decrypt data.
     */
    private final SecretKey secretKey;

    /**
     * Cipher implementation used to encrypt and decrypt data.
     */
    private final SupportedCipherDetails cipherDetails;

    /**
     * Creates a new instance of the helper class.
     *
     * @param connectionContext saved context used between requests to the Manta client
     * @param connectionFactory instance used for building requests to Manta
     * @param validateUploads flag toggling the checksum verification of uploaded files
     * @param encryptionKeyId the unique identifier of the key used for encryption
     * @param permitUnencryptedDownloads true when downloading unencrypted files is allowed in encryption mode
     * @param encryptionAuthenticationMode specifies if we are in strict ciphertext authentication mode or not
     * @param secretKey secret key used to encrypt and decrypt data
     * @param cipherDetails cipher implementation used to encrypt and decrypt data.
     */
    public EncryptionHttpHelper(final MantaConnectionContext connectionContext,
                                final MantaConnectionFactory connectionFactory,
                                final boolean validateUploads,
                                final String encryptionKeyId,
                                final boolean permitUnencryptedDownloads,
                                final EncryptionObjectAuthenticationMode encryptionAuthenticationMode,
                                final SecretKey secretKey,
                                final SupportedCipherDetails cipherDetails) {
        super(connectionContext, connectionFactory, validateUploads);

        this.encryptionKeyId = encryptionKeyId;
        this.permitUnencryptedDownloads = permitUnencryptedDownloads;
        this.encryptionAuthenticationMode = encryptionAuthenticationMode;
        this.secretKey = secretKey;
        this.cipherDetails = cipherDetails;
    }
}
