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
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
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
    public HttpResponse httpHead(final String path) throws IOException {
        HttpResponse response = super.httpHead(path);
        attachMetadata(response);

        return response;
    }

    @Override
    public HttpResponse httpGet(final String path) throws IOException {
        HttpResponse response = super.httpGet(path);
        attachMetadata(response);

        return response;
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
                secretKey, encryptionCipherDetails, originalEntity);

        final MantaMetadata metadata;

        if (originalMetadata != null) {
            metadata = originalMetadata;
        } else {
            metadata = new MantaMetadata();
        }

        /* We rewrite in the content-type so that from the perspective of the API consumer,
         * they are seeing the object written as if it wasn't encrypted. */
        String contentType = findOriginalContentType(originalEntity, httpHeaders);

        // Only add the wrapped content type if it isn't explicitly set
        if (contentType != null && !metadata.containsKey(MantaHttpHeaders.ENCRYPTED_CONTENT_TYPE)) {
            metadata.put(MantaHttpHeaders.ENCRYPTED_CONTENT_TYPE, contentType);
        }

        // Insert all of the headers needed for identifying the ciphers and HMACs used to encrypt
        attachEncryptionCipherHeaders(metadata);
        // Insert all of the headers and metadata needed for describing the encrypted entity
        attachEncryptedEntityHeaders(metadata, encryptingEntity);
        // Insert all of the encrypted metadata values into the metadata map
        attachEncryptedMetadata(metadata);

        MantaObjectResponse response = super.httpPut(path, httpHeaders, encryptingEntity, metadata);

        /* Emulate the setting of the content-type to our API consumer - when
         * in fact we are setting a different content type. */
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
         *
         * We only append plaintext content-length header if we are using an algorithm
         * that doesn't support an accurate calculation of plaintext content length in
         * order to minimize the calls per operation made to Manta.
         */

        // content-length of -1 means we are sending in chunked mode
        if (originalEntity.getContentLength() < 0 && encryptionCipherDetails.plaintextSizeCalculationIsAnEstimate()) {
            appendPlaintextContentLength(path, encryptingEntity, metadata, response);
        }

        return response;
    }

    @Override
    public MantaObjectInputStream httpRequestAsInputStream(final HttpUriRequest request,
                                                           final MantaHttpHeaders requestHeaders)
            throws IOException {
        if (requestHeaders != null && requestHeaders.getRange() != null
                && encryptionAuthenticationMode.equals(EncryptionAuthenticationMode.Mandatory)) {
            String msg = "HTTP range requests (random reads) aren't supported when using "
                    + "client-side encryption in mandatory authentication mode.";
            MantaClientEncryptionException e = new MantaClientEncryptionException(msg);
            HttpHelper.annotateContextedException(e, request, null);
            throw e;
        }

        final MantaObjectInputStream rawStream = super.httpRequestAsInputStream(request, requestHeaders);
        @SuppressWarnings("unchecked")
        final HttpResponse response = (HttpResponse)rawStream.getHttpResponse();

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
                HttpHelper.annotateContextedException(e, request, response);
                throw e;
            }
        }

        final String encryptionType = rawStream.getHeaderAsString(MantaHttpHeaders.ENCRYPTION_TYPE);
        final String metadataIvBase64 = rawStream.getHeaderAsString(MantaHttpHeaders.ENCRYPTION_METADATA_IV);
        final String metadataCiphertextBase64 = rawStream.getHeaderAsString(MantaHttpHeaders.ENCRYPTION_METADATA);
        final String hmacId = rawStream.getHeaderAsString(MantaHttpHeaders.ENCRYPTION_HMAC_TYPE);
        final String metadataHmacBase64 = rawStream.getHeaderAsString(MantaHttpHeaders.ENCRYPTION_METADATA_HMAC);

        Map<String, String> encryptedMetadata = buildEncryptedMetadata(
                encryptionType, metadataIvBase64, cipherId,
                metadataCiphertextBase64, hmacId, metadataHmacBase64, request, response);

        rawStream.getMetadata().putAll(encryptedMetadata);

        return new MantaEncryptedObjectInputStream(rawStream, secretKey, true);
    }

    @Override
    public MantaObjectResponse httpPutMetadata(final String path,
                                               final MantaHttpHeaders headers,
                                               final MantaMetadata metadata)
            throws IOException {
        /* Since metadata operations in Manta are always a replace operation,
         * we have to get the current metadata for the object and persist
         * the encryption-specific metadata headers. While at the same time
         * overwriting all other metadata values. Unfortunately, this process
         * requires two steps. */
        HttpResponse response = httpHead(path);

        boolean isEncryptedObject = response.getFirstHeader(MantaHttpHeaders.ENCRYPTION_CIPHER) != null;
        Header contentType = response.getFirstHeader(HttpHeaders.CONTENT_TYPE);
        if (contentType == null) {
            MantaIOException e = new MantaIOException("Content-Type value expected from Manta unavailable");
            HttpHelper.annotateContextedException(e, null, response);
            throw e;
        }

        boolean isDirectory = contentType.getValue().equals(MantaObjectResponse.DIRECTORY_RESPONSE_CONTENT_TYPE);

        // Return back the default implementation if the object is unencrypted or is a directory
        if (!isEncryptedObject || isDirectory) {
            return super.httpPutMetadata(path, headers, metadata);
        }

        /* Detect if the object we are operating on is encrypted, if not just
         * perform the default operation. */
        if (response.getFirstHeader(MantaHttpHeaders.ENCRYPTION_CIPHER) == null) {
            return super.httpPutMetadata(path, headers, metadata);
        }

        Header etag = response.getFirstHeader(HttpHeaders.ETAG);
        if (etag == null) {
            MantaIOException e = new MantaIOException("ETag value expected from Manta unavailable");
            HttpHelper.annotateContextedException(e, null, response);
            throw e;
        }

        Header lastModified = response.getFirstHeader(HttpHeaders.LAST_MODIFIED);
        if (lastModified == null) {
            MantaIOException e = new MantaIOException("Last-Modified value expected from Manta unavailable");
            HttpHelper.annotateContextedException(e, null, response);
            throw e;
        }

        Header actualContentType = response.getFirstHeader(MantaHttpHeaders.ENCRYPTED_CONTENT_TYPE);

        /* We add the encrypted content-type value back into the metadata if
         * it wasn't explicitly added to the metadata to replace, so that the
         * ciphertext's content-type will remain consistent. */
        if (actualContentType != null) {
            metadata.putIfAbsent(MantaHttpHeaders.ENCRYPTED_CONTENT_TYPE,
                    actualContentType.getValue());
        }

        for (String h : MantaHttpHeaders.ENCRYPTED_ENTITY_HEADERS) {
            final Header header = response.getFirstHeader(h);
            if (header == null) {
                continue;
            }

            metadata.putIfAbsent(h, header.getValue());
        }

        headers.put(HttpHeaders.IF_MATCH, etag.getValue());
        headers.put(HttpHeaders.IF_UNMODIFIED_SINCE, lastModified.getValue());

        attachEncryptionCipherHeaders(metadata);
        attachEncryptedMetadata(metadata);
        return super.httpPutMetadata(path, headers, metadata);
    }

    /**
     * Attaches encrypted metadata headers to an HTTP response.
     * @param response response to attach metadata to
     */
    private void attachMetadata(final HttpResponse response) {
        Header contentTypeHeader = response.getFirstHeader(HttpHeaders.CONTENT_TYPE);
        final String contentType;

        if (contentTypeHeader == null) {
            contentType = null;
        } else {
            contentType = contentTypeHeader.getValue();
        }

        // No encryption operations are needed on directories, so we just pass
        // back the object as is
        if (contentType != null && contentType.equals(MantaObjectResponse.DIRECTORY_RESPONSE_CONTENT_TYPE)) {
            return;
        }

        Map<String, String> encryptedMetadata = extractEncryptionHeadersFromResponse(response);

        /* Object is not encrypted - since we aren't downloading anything, we
         * assume a peek at the headers is safe. We will just pass along the
         * response value with no additional modifications. */
        if (encryptedMetadata == null) {
            return;
        }

        for (Map.Entry<String, String> entry : encryptedMetadata.entrySet()) {
            response.setHeader(entry.getKey(), entry.getValue());
        }

        String encryptedContentType = encryptedMetadata.get(MantaHttpHeaders.ENCRYPTED_CONTENT_TYPE);
        if (encryptedContentType != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Encrypted content-type [{}] overwriting returned content-type [{}]",
                        encryptedContentType, response.getFirstHeader(HttpHeaders.CONTENT_TYPE));
            }
            response.setHeader(HttpHeaders.CONTENT_TYPE, encryptedContentType);
        }
    }

    /**
     * Finds the headers used for encryption, parses their values and
     * converts them to a {@link Map}.
     *
     * @param response response object to parse
     * @return map containing encryption headers and values or null if unencrypted object
     */
    private Map<String, String> extractEncryptionHeadersFromResponse(
            final HttpResponse response) {
        String cipherId = null;
        String encryptionType = null;
        String metadataIvBase64 = null;
        String metadataCiphertextBase64 = null;
        String hmacId = null;
        String metadataHmacBase64 = null;

        final Header[] headers = response.getAllHeaders();
        for (final Header h : headers) {
            // Don't bother to parse anything that isn't Manta specific metadata
            if (!h.getName().startsWith("m-")) {
                continue;
            }

            switch (h.getName()) {
                case MantaHttpHeaders.ENCRYPTION_TYPE:
                    encryptionType = h.getValue();
                    continue;
                case MantaHttpHeaders.ENCRYPTION_METADATA_IV:
                    metadataIvBase64 = h.getValue();
                    continue;
                case MantaHttpHeaders.ENCRYPTION_CIPHER:
                    cipherId = h.getValue();
                    continue;
                case MantaHttpHeaders.ENCRYPTION_METADATA:
                    metadataCiphertextBase64 = h.getValue();
                    continue;
                case MantaHttpHeaders.ENCRYPTION_HMAC_TYPE:
                    hmacId = h.getValue();
                    continue;
                case MantaHttpHeaders.ENCRYPTION_METADATA_HMAC:
                    metadataHmacBase64 = h.getValue();
                    continue;
                default:
            }
        }

        if (cipherId == null) {
            return null;
        }

        return buildEncryptedMetadata(
                encryptionType, metadataIvBase64, cipherId,
                metadataCiphertextBase64, hmacId, metadataHmacBase64, null, response);
    }

    /**
     * Builds a {@link Map} of decrypted metadata keys and values.
     *
     * @param encryptionType encryption type header value
     * @param metadataIvBase64 metadata ciphertext iv header value
     * @param metadataCipherId metadata cipher identifier header value
     * @param metadataCiphertextBase64 metadata ciphertext header value
     * @param hmacId hmac identifier header value
     * @param metadataHmacBase64 metadata hmac header value
     * @param request http request object
     * @param response http response object
     * @return decrypted map of encrypted metadata
     */
    @SuppressWarnings("ParameterNumber")
    private Map<String, String> buildEncryptedMetadata(final String encryptionType,
                                                       final String metadataIvBase64,
                                                       final String metadataCipherId,
                                                       final String metadataCiphertextBase64,
                                                       final String hmacId,
                                                       final String metadataHmacBase64,
                                                       final HttpRequest request,
                                                       final HttpResponse response) {
        try {
            EncryptionType.validateEncryptionTypeIsSupported(encryptionType);
        } catch (MantaClientEncryptionException e) {
            HttpHelper.annotateContextedException(e, request, response);
            throw e;
        }

        byte[] metadataIv = Base64.getDecoder().decode(metadataIvBase64);

        if (metadataCipherId == null) {
            String msg = "No metadata cipher specified";
            MantaClientEncryptionException e = new MantaClientEncryptionException(msg);
            HttpHelper.annotateContextedException(e, request, response);
            throw e;
        }

        SupportedCipherDetails metadataCipherDetails = SupportedCiphersLookupMap.INSTANCE.get(
                metadataCipherId);

        if (metadataCipherDetails == null) {
            String msg = String.format("Unsupported metadata cipher specified: %s",
                    metadataCipherId);
            MantaClientEncryptionException e = new MantaClientEncryptionException(msg);
            HttpHelper.annotateContextedException(e, request, response);
            throw e;
        }

        Cipher metadataCipher = buildMetadataDecryptCipher(metadataCipherDetails, metadataIv);

        if (metadataCiphertextBase64 == null) {
            String msg = "No encrypted metadata stored on object";
            MantaClientEncryptionException e = new MantaClientEncryptionException(msg);
            HttpHelper.annotateContextedException(e, request, response);
            throw e;
        }

        byte[] metadataCipherText = Base64.getDecoder().decode(metadataCiphertextBase64);

        // Validate Hmac if we aren't using AEAD
        if (!encryptionCipherDetails.isAEADCipher()) {
            if (hmacId == null) {
                String msg = "No HMAC algorithm specified for metadata ciphertext authentication";
                MantaClientEncryptionException e = new MantaClientEncryptionException(msg);
                HttpHelper.annotateContextedException(e, request, response);
                throw e;
            }

            Supplier<Mac> hmacSupplier = SupportedHmacsLookupMap.INSTANCE.get(hmacId);
            if (hmacSupplier == null) {
                String msg = String.format("Unsupported HMAC specified: %s",
                        hmacId);
                MantaClientEncryptionException e = new MantaClientEncryptionException(msg);
                HttpHelper.annotateContextedException(e, request, response);
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

            if (metadataHmacBase64 == null) {
                String msg = "No metadata HMAC is available to authenticate metadata ciphertext";
                MantaClientEncryptionException e = new MantaClientEncryptionException(msg);
                HttpHelper.annotateContextedException(e, request, response);
                throw e;
            }

            byte[] expectedHmac = Base64.getDecoder().decode(metadataHmacBase64);

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
        return EncryptedMetadataUtils.plaintextMetadataAsMap(plaintext);
    }

    /**
     * Adds headers and metadata needed for client-side encryption to a request
     * (typically a PUT).
     *
     * @param metadata metadata to append additional values to
     */
    private void attachEncryptionCipherHeaders(final MantaMetadata metadata) {
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
    }

    /**
     * Adds headers related directly to the encrypted object being stored.
     *
     * @param metadata Manta metadata object
     * @param encryptingEntity HTTP Entity object that encrypts the Manta object data
     * @throws IOException thrown when unable to append metadata
     */
    private void attachEncryptedEntityHeaders(final MantaMetadata metadata,
                                              final EncryptingEntity encryptingEntity)
            throws IOException {
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
    }

    /**
     * Attaches encrypted metadata (with e-* values) to the object.
     *
     * @param metadata metadata to append additional values to
     * @throws IOException thrown when there is a problem attaching metadata
     */
    private void attachEncryptedMetadata(final MantaMetadata metadata)
        throws IOException {

        // Create and add encrypted metadata
        Cipher metadataCipher = buildMetadataEncryptCipher(this.encryptionCipherDetails);

        metadata.put(MantaHttpHeaders.ENCRYPTION_CIPHER,
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

            byte[] checksum = hmac.doFinal(metadataCipherText);
            String checksumBase64 = Base64.getEncoder().encodeToString(checksum);
            metadata.put(MantaHttpHeaders.ENCRYPTION_METADATA_HMAC, checksumBase64);

            LOGGER.debug("Encrypted metadata HMAC: {}", checksumBase64);
        }
    }

    /**
     * Looks up the content-type used in the entity being encrypted.
     *
     * @param originalEntity reference to original entity
     * @param httpHeaders reference to http headers that may also have a content-type
     * @return the content-type found or null if not found
     */
    private String findOriginalContentType(final HttpEntity originalEntity,
                                           final MantaHttpHeaders httpHeaders) {
        final String entityContentType;

        if (originalEntity.getContentType() == null) {
            entityContentType = null;
        } else {
            entityContentType = originalEntity.getContentType().getValue();
        }

        return ObjectUtils.firstNonNull(httpHeaders.getContentType(),
                entityContentType);
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
     * race condition if the clock has not incremented one second and another call
     * came and updated the metadata before this call was performed because the original
     * call because the <code>If-Unmodified-Since</code> header only has a resolution
     * of seconds. This means that in some cases the metadata may overwrite other
     * changes if you are doing metadata modifications from another client at
     * very low latencies that land right after the object is added. This is
     * a hypothetical and unlikely scenario.</p>
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
        byte[] metadataIv = cipherDetails.generateIv();
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
