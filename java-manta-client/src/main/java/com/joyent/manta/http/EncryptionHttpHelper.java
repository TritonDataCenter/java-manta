/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.http;

import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.client.crypto.EncryptedMetadataUtils;
import com.joyent.manta.client.crypto.EncryptingEntity;
import com.joyent.manta.client.crypto.EncryptionType;
import com.joyent.manta.client.crypto.MantaEncryptedObjectInputStream;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.crypto.SupportedCiphersLookupMap;
import com.joyent.manta.client.crypto.SupportedHmacsLookupMap;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.EncryptionAuthenticationMode;
import com.joyent.manta.exception.MantaClientEncryptionException;
import com.joyent.manta.exception.MantaIOException;
import com.joyent.manta.http.entity.NoContentEntity;
import org.apache.commons.codec.Charsets;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

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
     * Maximum size of metadata ciphertext in bytes.
     */
    private static final int MAX_METADATA_CIPHERTEXT_BASE64_SIZE = 4_000;

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
     * Cipher implementation used to encrypt data.
     */
    private final SupportedCipherDetails encryptionCipherDetails;

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

        this.encryptionCipherDetails = ObjectUtils.firstNonNull(
                SupportedCiphersLookupMap.INSTANCE.getWithCaseInsensitiveKey(
                        config.getEncryptionAlgorithm()), DefaultsConfigContext.DEFAULT_CIPHER);

        if (config.getEncryptionPrivateKeyPath() != null) {
            Path keyPath = Paths.get(config.getEncryptionPrivateKeyPath());

            try {
                secretKey = SecretKeyUtils.loadKeyFromPath(keyPath,
                        this.encryptionCipherDetails);
            } catch (IOException e) {
                String msg = String.format("Unable to load secret key from file: %s",
                        keyPath);
                throw new UncheckedIOException(msg, e);
            }
        } else if (config.getEncryptionPrivateKeyBytes() != null) {
            secretKey = SecretKeyUtils.loadKey(config.getEncryptionPrivateKeyBytes(),
                    encryptionCipherDetails);
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
                secretKey, encryptionCipherDetails, originalEntity, secureRandom
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

        // Encryption type identifier
        metadata.put(MantaHttpHeaders.ENCRYPTION_TYPE, EncryptionType.CLIENT.toString());
        LOGGER.debug("Encryption type: {}", EncryptionType.CLIENT);

        // Encryption Cipher
        metadata.put(MantaHttpHeaders.ENCRYPTION_CIPHER,
                encryptionCipherDetails.getCipherId());
        LOGGER.debug("Encryption cipher: {}", encryptionCipherDetails.getCipherId());

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
        if (encryptionCipherDetails.isAEADCipher()) {
            metadata.put(MantaHttpHeaders.ENCRYPTION_AEAD_TAG_LENGTH,
                    String.valueOf(encryptionCipherDetails.getAuthenticationTagOrHmacLengthInBytes()));
            LOGGER.debug("AEAD tag length: {}", encryptionCipherDetails.getAuthenticationTagOrHmacLengthInBytes());
        // HMAC Type because we are doing MtE
        } else {
            Mac hmac = encryptionCipherDetails.getAuthenticationHmac();
            metadata.put(MantaHttpHeaders.ENCRYPTION_HMAC_TYPE,
                    hmac.getAlgorithm());
            LOGGER.debug("HMAC algorithm: {}", hmac.getAlgorithm());
        }

        // Add default metadata values
        final String entityContentType;
        if (originalEntity.getContentType() == null) {
            entityContentType = null;
        } else {
            entityContentType = originalEntity.getContentType().getValue();
        }

        String contentType = ObjectUtils.firstNonNull(
                httpHeaders.getContentType(),
                entityContentType);

        if (contentType != null && !metadata.containsKey("e-content-type")) {
            metadata.put("e-content-type", contentType);
        }

        // Create and add encrypted metadata
        Cipher metadataCipher = buildMetadataEncryptCipher(this.encryptionCipherDetails);

        httpHeaders.put(MantaHttpHeaders.ENCRYPTION_CIPHER,
                encryptionCipherDetails.getCipherId());

        String metadataIvBase64 = Base64.getEncoder().encodeToString(
                metadataCipher.getIV());
        metadata.put(MantaHttpHeaders.ENCRYPTION_METADATA_IV, metadataIvBase64);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Encrypted metadata IV: {}",
                    Hex.encodeHexString(metadataCipher.getIV()));
        }

        String metadataPlainTextString = EncryptedMetadataUtils.encryptedMetadataAsString(metadata);
        LOGGER.debug("Encrypted metadata plaintext:\n{}", metadataPlainTextString);
        LOGGER.debug("Encrypted metadata ciphertext: {}", metadataIvBase64);

        byte[] metadataCipherText = encryptMetadata(metadataPlainTextString, metadataCipher);
        String metadataCipherTextBase64 = Base64.getEncoder().encodeToString(
                metadataCipherText);

        if (metadataCipherTextBase64.length() > MAX_METADATA_CIPHERTEXT_BASE64_SIZE) {
            String msg = "Encrypted metadata exceeded the maximum size allowed";
            MantaClientEncryptionException e = new MantaClientEncryptionException(msg);
            e.setContextValue("max_size", MAX_METADATA_CIPHERTEXT_BASE64_SIZE);
            e.setContextValue("actual_size", metadataCipherTextBase64.length());
            throw e;
        }

        metadata.put(MantaHttpHeaders.ENCRYPTION_METADATA, metadataCipherTextBase64);

        if (!encryptionCipherDetails.isAEADCipher()) {
            Mac hmac = encryptionCipherDetails.getAuthenticationHmac();
            byte[] checksum = hmac.doFinal(metadataCipherText);
            String checksumBase64 = Base64.getEncoder().encodeToString(checksum);
            metadata.put(MantaHttpHeaders.ENCRYPTION_METADATA_HMAC, checksumBase64);

            LOGGER.debug("Encrypted metadata HMAC: {}", checksumBase64);
        }

        MantaObjectResponse response = super.httpPut(path, httpHeaders, encryptingEntity, metadata);

        /* We rewrite in the content-type so that from the perspective of the API consumer,
         * they are seeing the object written as if it wasn't encrypted. */
        response.setContentType(contentType);

        /* If we sent over an entity where it was impossible to know the original size until
         * we finished streaming, then we do an additional call to update the metadata of the
         * object so that the plaintext size is stored.
         *
         * Having this data available allows MantaEncryptedObjectInputStream.getContentLength()
         * to return the actual plaintext value. Most of the time, this value can be gotten
         * via a calculation based on the ciphertext size. However, in the cases of
         * the AES/CBC ciphers, we can't calculate the plaintext size via the ciphertext
         * size, so we do a metadata update call to update the value.
         */

        // content-length of -1 means we are sending in chunked mode
        if (originalEntity.getContentLength() < 0 && encryptionCipherDetails.plaintextSizeCalculationIsAnEstimate()) {
            appendPlaintextContentLength(path, encryptingEntity, metadata, response);
        }

        return response;
    }

    /**
     * <p>Performs a conditional call to update the metadata for an object to add the
     * <code>m-encrypt-plaintext-content-length</code> header to the object. This
     * method is used after an object is streamed to Manta in chunked mode and we
     * only end up knowing its plaintext content length size when the transfer
     * completed.</p>
     *
     * <p>This method makes use of the <code>If-Match</code> and
     * <code>If-Unmodified-Since</code> HTTP headers. There is the possibility of a
     * race condition if the clock has not incremented one second since the original
     * call because the <code>If-Unmodified-Since</code> header only has a resolution
     * of seconds. This means that in some cases the metadata will not be updated.</p>
     *
     * @param path path to the object
     * @param encryptingEntity the encrypting entity that streamed the object
     * @param metadata the metadata object from the original transfer
     * @param response the original response object from the original transfer
     * @throws IOException thrown when we are unable to update the metadata
     */
    private void appendPlaintextContentLength(final String path,
                                              final EncryptingEntity encryptingEntity,
                                              final MantaMetadata metadata,
                                              final MantaObjectResponse response) throws IOException {
        List<NameValuePair> pairs = Collections.singletonList(new BasicNameValuePair("metadata", "true"));
        HttpPut put = getConnectionFactory().put(path, pairs);
        metadata.put(MantaHttpHeaders.ENCRYPTION_PLAINTEXT_CONTENT_LENGTH,
                String.valueOf(encryptingEntity.getOriginalLength()));

        MantaHttpHeaders updateHeaders = new MantaHttpHeaders();
        updateHeaders.putAll(metadata);
        updateHeaders.put(HttpHeaders.IF_MATCH, response.getEtag());
        updateHeaders.put(HttpHeaders.IF_UNMODIFIED_SINCE, response.getLastModifiedTime());

        put.setHeaders(updateHeaders.asApacheHttpHeaders());
        put.setEntity(NoContentEntity.INSTANCE);

        CloseableHttpClient client = getConnectionContext().getHttpClient();
        CloseableHttpResponse originalContentLengthUpdateResponse = client.execute(put);
        IOUtils.closeQuietly(originalContentLengthUpdateResponse);

        StatusLine statusLine = originalContentLengthUpdateResponse.getStatusLine();
        int code = statusLine.getStatusCode();

        if (code != HttpStatus.SC_NO_CONTENT && code != HttpStatus.SC_PRECONDITION_FAILED) {
            MantaIOException e = new MantaIOException("Unable to update metadata with"
                    + " original plaintext content length");
            HttpHelper.annotateContextedException(e, put, originalContentLengthUpdateResponse);
            throw e;
        }
    }

    @Override
    public MantaObjectInputStream httpRequestAsInputStream(final HttpUriRequest request,
                                                           final MantaHttpHeaders requestHeaders)
            throws IOException {
        MantaObjectInputStream rawStream = super.httpRequestAsInputStream(request, requestHeaders);

        try {
            EncryptionType.validateEncryptionTypeIsSupported(rawStream.getHeaderAsString(
                    MantaHttpHeaders.ENCRYPTION_TYPE));
        } catch (MantaClientEncryptionException e) {
            HttpHelper.annotateContextedException(e, request, null);
            throw e;
        }

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

        String metadataIvBase64 = rawStream.getHeaderAsString(MantaHttpHeaders.ENCRYPTION_METADATA_IV);
        byte[] metadataIv = Base64.getDecoder().decode(metadataIvBase64);

        String metadataCipherId = rawStream.getHeaderAsString(MantaHttpHeaders.ENCRYPTION_CIPHER);

        if (metadataCipherId == null) {
            String msg = "No metadata cipher specified";
            MantaClientEncryptionException e = new MantaClientEncryptionException(msg);
            HttpHelper.annotateContextedException(e, request, null);
            throw e;
        }

        SupportedCipherDetails metadataCipherDetails = SupportedCiphersLookupMap.INSTANCE.get(
                metadataCipherId);

        if (metadataCipherDetails == null) {
            String msg = String.format("Unsupported metadata cipher specified: %s",
                    metadataCipherId);
            MantaClientEncryptionException e = new MantaClientEncryptionException(msg);
            HttpHelper.annotateContextedException(e, request, null);
            throw e;
        }

        Cipher metadataCipher = buildMetadataDecryptCipher(metadataCipherDetails, metadataIv);

        String metadataCiphertextBase64 = rawStream.getHeaderAsString(MantaHttpHeaders.ENCRYPTION_METADATA);

        if (metadataCiphertextBase64 == null) {
            String msg = "No encrypted metadata stored on object";
            MantaClientEncryptionException e = new MantaClientEncryptionException(msg);
            HttpHelper.annotateContextedException(e, request, null);
            throw e;
        }

        byte[] metadataCipherText = Base64.getDecoder().decode(metadataCiphertextBase64);

        // Validate Hmac if we aren't using AEAD
        if (!encryptionCipherDetails.isAEADCipher()) {
            String hmacId = rawStream.getHeaderAsString(MantaHttpHeaders.ENCRYPTION_HMAC_TYPE);

            if (hmacId == null) {
                String msg = "No HMAC algorithm specified for metadata ciphertext authentication";
                MantaClientEncryptionException e = new MantaClientEncryptionException(msg);
                HttpHelper.annotateContextedException(e, request, null);
                throw e;
            }

            Supplier<Mac> hmacSupplier = SupportedHmacsLookupMap.INSTANCE.get(hmacId);
            if (hmacSupplier == null) {
                String msg = String.format("Unsupported HMAC specified: %s",
                        hmacId);
                MantaClientEncryptionException e = new MantaClientEncryptionException(msg);
                HttpHelper.annotateContextedException(e, request, null);
                throw e;
            }

            Mac hmac = hmacSupplier.get();

            try {
                hmac.init(secretKey);
            } catch (InvalidKeyException e) {
                MantaClientEncryptionException mcee = new MantaClientEncryptionException(
                        "There was a problem loading private key", e);
                String details = String.format("key=%s, algorithm=%s",
                        secretKey.getAlgorithm(), secretKey.getFormat());
                mcee.setContextValue("key_details", details);
                throw mcee;
            }

            byte[] actualHmac = hmac.doFinal(metadataCipherText);

            String hmacBase64 = rawStream.getHeaderAsString(MantaHttpHeaders.ENCRYPTION_METADATA_HMAC);

            if (hmacBase64 == null) {
                String msg = "No metadata HMAC is available to authenticate metadata ciphertext";
                MantaClientEncryptionException e = new MantaClientEncryptionException(msg);
                HttpHelper.annotateContextedException(e, request, null);
                throw e;
            }

            byte[] expectedHmac = Base64.getDecoder().decode(hmacBase64);

            if (!Arrays.equals(expectedHmac, actualHmac)) {
                String msg = "The expected HMAC value for metadata ciphertext didn't equal the actual value";
                MantaClientEncryptionException e = new MantaClientEncryptionException(msg);
                HttpHelper.annotateContextedException(e, request, null);
                e.setContextValue("expected", Hex.encodeHexString(expectedHmac));
                e.setContextValue("actual", Hex.encodeHexString(actualHmac));
                throw e;
            }
        }

        byte[] plaintext = decryptMetadata(metadataCipherText, metadataCipher);
        Map<String, String> encryptedMetadata = EncryptedMetadataUtils.plaintextMetadataAsMap(plaintext);
        rawStream.getMetadata().putAll(encryptedMetadata);

        return new MantaEncryptedObjectInputStream(rawStream, secretKey);
    }

    /**
     * Encrypts a plaintext object metadata string.
     *
     * @param metadataPlaintext string to encrypt
     * @param cipher cipher to use for encryption
     * @return byte array of ciphertext
     */
    private byte[] encryptMetadata(final String metadataPlaintext, final Cipher cipher) {
        byte[] rawBytes = metadataPlaintext.getBytes(Charsets.US_ASCII);

        try {
            return cipher.doFinal(rawBytes);
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            MantaClientEncryptionException mcee = new MantaClientEncryptionException(
                    "There was a problem encrypting the object's metadata", e);
            String details = String.format("key=%s, algorithm=%s",
                    secretKey.getAlgorithm(), secretKey.getFormat());
            mcee.setContextValue("key_details", details);
            throw mcee;
        }
    }

    /**
     * Decrypts metadata ciphertext.
     *
     * @param metadataCiphertext ciphertext to decrypt
     * @param cipher cipher to use for decryption
     * @return raw plaintext in binary
     */
    private byte[] decryptMetadata(final byte[] metadataCiphertext, final Cipher cipher) {
        try {
            return cipher.doFinal(metadataCiphertext);
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            MantaClientEncryptionException mcee = new MantaClientEncryptionException(
                    "There was a problem decrypting the object's metadata", e);
            String details = String.format("key=%s, algorithm=%s",
                    secretKey.getAlgorithm(), secretKey.getFormat());
            mcee.setContextValue("key_details", details);
            throw mcee;
        }
    }

    /**
     * Configures and instantiates the cipher object used for encrypting object
     * metadata.
     *
     * @param cipherDetails cipher to use for encryption
     * @return a configured cipher instance
     */
    private Cipher buildMetadataEncryptCipher(final SupportedCipherDetails cipherDetails) {
        byte[] metadataIv = new byte[cipherDetails.getIVLengthInBytes()];
        secureRandom.nextBytes(metadataIv);
        Cipher metadataCipher = cipherDetails.getCipher();
        try {
            AlgorithmParameterSpec spec = cipherDetails.getEncryptionParameterSpec(metadataIv);
            metadataCipher.init(Cipher.ENCRYPT_MODE, secretKey, spec);
        } catch (InvalidKeyException e) {
            MantaClientEncryptionException mcee = new MantaClientEncryptionException(
                    "There was a problem loading private key", e);
            String details = String.format("key=%s, algorithm=%s",
                    secretKey.getAlgorithm(), secretKey.getFormat());
            mcee.setContextValue("key_details", details);
            throw mcee;
        } catch (InvalidAlgorithmParameterException e) {
            throw new MantaClientEncryptionException(
                    "There was a problem with the passed algorithm parameters", e);
        }
        return metadataCipher;
    }

    /**
     * Configures and instantiates the cipher object used for decrypting object
     * metadata.
     *
     * @param cipherDetails cipher to use for encryption
     * @param metadataIv encrypted metadata initialization vector
     * @return a configured cipher instance
     */
    private Cipher buildMetadataDecryptCipher(final SupportedCipherDetails cipherDetails,
                                              final byte[] metadataIv) {
        Cipher metadataCipher = cipherDetails.getCipher();
        try {
            AlgorithmParameterSpec spec = cipherDetails.getEncryptionParameterSpec(metadataIv);
            metadataCipher.init(Cipher.DECRYPT_MODE, secretKey, spec);
        } catch (InvalidKeyException e) {
            MantaClientEncryptionException mcee = new MantaClientEncryptionException(
                    "There was a problem loading private key", e);
            String details = String.format("key=%s, algorithm=%s",
                    secretKey.getAlgorithm(), secretKey.getFormat());
            mcee.setContextValue("key_details", details);
            throw mcee;
        } catch (InvalidAlgorithmParameterException e) {
            throw new MantaClientEncryptionException(
                    "There was a problem with the passed algorithm parameters", e);
        }
        return metadataCipher;
    }
}
