package com.joyent.manta.http;

import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.EncryptionAuthenticationMode;
import org.apache.commons.lang3.ObjectUtils;

import javax.crypto.SecretKey;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

import static com.joyent.manta.client.crypto.SupportedCipherDetails.SUPPORTED_CIPHERS;

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
    private final EncryptionAuthenticationMode encryptionAuthenticationMode;

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
     * @param config configuration context object
     */
    public EncryptionHttpHelper(final MantaConnectionContext connectionContext,
                                final MantaConnectionFactory connectionFactory,
                                final ConfigContext config) {
        super(connectionContext, connectionFactory, config);

        this.encryptionKeyId = ObjectUtils.firstNonNull(
                config.getEncryptionKeyId(), "unknown-key");
        this.permitUnencryptedDownloads = ObjectUtils.firstNonNull(
                config.permitUnencryptedDownloads(),
                DefaultsConfigContext.DEFAULT_PERMIT_UNENCRYPTED_DOWNLOADS
        );

        this.encryptionAuthenticationMode = ObjectUtils.firstNonNull(
                config.getEncryptionAuthenticationMode(),
                EncryptionAuthenticationMode.DEFAULT_MODE);

        this.cipherDetails = ObjectUtils.firstNonNull(
                SUPPORTED_CIPHERS.getWithCaseInsensitiveKey(config.getEncryptionAlgorithm()),
                DefaultsConfigContext.DEFAULT_CIPHER
        );

        if (config.getEncryptionPrivateKeyPath() != null) {
            File keyFile = new File(config.getEncryptionPrivateKeyPath());

            try {
                secretKey = SecretKeyUtils.loadKeyFromFile(keyFile,
                        this.cipherDetails);
            } catch (IOException e) {
                String msg = String.format("Unable to load secret key from file: %s",
                        keyFile.getAbsolutePath());
                throw new UncheckedIOException(msg, e);
            }
        } else if (config.getEncryptionPrivateKeyBytes() != null) {
            secretKey = SecretKeyUtils.loadKey(config.getEncryptionPrivateKeyBytes(),
                    cipherDetails);
        } else {
            throw new IllegalStateException("Either private key path or bytes must be specified");
        }
    }
}
