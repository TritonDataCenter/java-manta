/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.http;

import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.client.crypto.EncryptingEntity;
import com.joyent.manta.client.crypto.MantaEncryptedObjectInputStream;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.crypto.SupportedCiphersLookupMap;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.EncryptionAuthenticationMode;
import com.joyent.manta.exception.MantaClientEncryptionException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpUriRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * {@link HttpHelper} implementation that transparently handles client-side
 * encryption when using the Manta server API.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class EncryptionHttpHelper extends StandardHttpHelper {
    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(EncryptionHttpHelper.class);

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
     * We use the default entropy source configured in the JVM.
     */
    private final SecureRandom secureRandom = new SecureRandom();

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
                SupportedCiphersLookupMap.INSTANCE.getWithCaseInsensitiveKey(
                        config.getEncryptionAlgorithm()), DefaultsConfigContext.DEFAULT_CIPHER);

        if (config.getEncryptionPrivateKeyPath() != null) {
            Path keyPath = Paths.get(config.getEncryptionPrivateKeyPath());

            try {
                secretKey = SecretKeyUtils.loadKeyFromPath(keyPath,
                        this.cipherDetails);
            } catch (IOException e) {
                String msg = String.format("Unable to load secret key from file: %s",
                        keyPath);
                throw new UncheckedIOException(msg, e);
            }
        } else if (config.getEncryptionPrivateKeyBytes() != null) {
            secretKey = SecretKeyUtils.loadKey(config.getEncryptionPrivateKeyBytes(),
                    cipherDetails);
        } else {
            throw new IllegalStateException("Either private key path or bytes must be specified");
        }
    }

    @Override
    public MantaObjectResponse httpPut(final String path,
                                       final MantaHttpHeaders headers,
                                       final HttpEntity originalEntity,
                                       final MantaMetadata originalMetadata) throws IOException {
        final MantaHttpHeaders httpHeaders;

        if (headers == null) {
            httpHeaders = new MantaHttpHeaders();
        } else {
            httpHeaders = headers;
        }

        EncryptingEntity encryptingEntity = new EncryptingEntity(
                secretKey, cipherDetails, originalEntity, secureRandom
        );

        final MantaMetadata metadata;

        if (originalMetadata != null) {
            metadata = originalMetadata;
        } else {
            metadata = new MantaMetadata();
        }

        // Secret Key ID
        metadata.put(MantaHttpHeaders.ENCRYPTION_KEY_ID,
                encryptionKeyId);
        LOGGER.debug("Secret key id: {}", encryptionKeyId);

        // Encryption Cipher
        metadata.put(MantaHttpHeaders.ENCRYPTION_CIPHER,
                cipherDetails.getCipherId());
        LOGGER.debug("Encryption cipher: {}", cipherDetails.getCipherId());

        // IV Used to Encrypt
        String ivBase64 = Base64.getEncoder().encodeToString(
                encryptingEntity.getCipher().getIV());
        metadata.put(MantaHttpHeaders.ENCRYPTION_IV, ivBase64);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("IV: {}", Hex.encodeHexString(encryptingEntity.getCipher().getIV()));
        }

        // Plaintext content-length if available
        if (encryptingEntity.getOriginalLength() > EncryptingEntity.UNKNOWN_LENGTH) {
            String originalLength = String.valueOf(encryptingEntity.getOriginalLength());
            metadata.put(MantaHttpHeaders.ENCRYPTION_PLAINTEXT_CONTENT_LENGTH,
                    originalLength);
            LOGGER.debug("Plaintext content-length: {}", originalLength);
        }

        // AEAD Tag Length if AEAD Cipher
        if (cipherDetails.isAEADCipher()) {
            metadata.put(MantaHttpHeaders.ENCRYPTION_AEAD_TAG_LENGTH,
                    String.valueOf(cipherDetails.getAuthenticationTagOrHmacLengthInBytes()));
            LOGGER.debug("AEAD tag length: {}", cipherDetails.getAuthenticationTagOrHmacLengthInBytes());
        // HMAC Type because we are doing MtE
        } else {
            Mac hmac = cipherDetails.getAuthenticationHmac();
            metadata.put(MantaHttpHeaders.ENCRYPTION_HMAC_TYPE,
                    hmac.getAlgorithm());
            LOGGER.debug("HMAC algorithm: {}", hmac.getAlgorithm());
        }

        // Create and add encrypted metadata

        return super.httpPut(path, httpHeaders, encryptingEntity, metadata);
    }

    @Override
    public MantaObjectInputStream httpRequestAsInputStream(final HttpUriRequest request,
                                                           final MantaHttpHeaders requestHeaders)
            throws IOException {
        MantaObjectInputStream rawStream = super.httpRequestAsInputStream(request, requestHeaders);

        final String cipherId = rawStream.getHeaderAsString(MantaHttpHeaders.ENCRYPTION_CIPHER);

        if (cipherId == null) {
            if (permitUnencryptedDownloads) {
                return rawStream;
            } else {
                String msg = "Unable to download a unencrypted file when "
                        + "client-side encryption is enabled unless the "
                        + "permit unencrypted downloads configuration setting "
                        + "is enabled";
                MantaClientEncryptionException e = new MantaClientEncryptionException(msg);
                HttpHelper.annotateContextedException(e, request, null);

                throw e;
            }
        }

        return new MantaEncryptedObjectInputStream(rawStream, secretKey);
    }
}
