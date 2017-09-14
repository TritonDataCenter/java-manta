/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.config;

import com.joyent.manta.util.MantaUtils;

import java.util.Arrays;
import java.util.Base64;
import java.util.Map;

import static com.joyent.manta.config.EnvVarConfigContext.*;

/**
 * {@link ConfigContext} implementation that is used for configuring instances
 * from a Map.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MapConfigContext implements ConfigContext {
    /**
     * Property key for looking up a Manta URL.
     */
    public static final String MANTA_URL_KEY = "manta.url";

    /**
     * Property key for looking up a Manta account.
     */
    public static final String MANTA_USER_KEY = "manta.user";

    /**
     * Property key for looking up a RSA fingerprint.
     */
    public static final String MANTA_KEY_ID_KEY = "manta.key_id";

    /**
     * Property key for looking up a RSA private key path.
     */
    public static final String MANTA_KEY_PATH_KEY = "manta.key_path";

    /**
     * Property key for looking up a Manta timeout.
     */
    public static final String MANTA_TIMEOUT_KEY = "manta.timeout";

    /**
     * Property key for number of times to retry failed requests.
     */
    public static final String MANTA_RETRIES_KEY = "manta.retries";

    /**
     * Property key for the maximum number of open connections to the Manta API.
     */
    public static final String MANTA_MAX_CONNS_KEY = "manta.max_connections";

    /**
     * Property key for looking up Manta private key content.
     */
    public static final String MANTA_PRIVATE_KEY_CONTENT_KEY = "manta.key_content";

    /**
     * Property key for looking up Manta password.
     */
    public static final String MANTA_PASSWORD_KEY = "manta.password";

    /**
     * Property key for setting HTTP buffer size.
     */
    public static final String MANTA_HTTP_BUFFER_SIZE_KEY = "manta.http_buffer_size";

    /**
     * Property key for setting TLS protocols.
     */
    public static final String MANTA_HTTPS_PROTOCOLS_KEY = "https.protocols";

    /**
     * Property key for setting TLS ciphers.
     */
    public static final String MANTA_HTTPS_CIPHERS_KEY = "https.cipherSuites";

    /**
     * Property key for disabling HTTP signatures.
     */
    public static final String MANTA_NO_AUTH_KEY = "manta.no_auth";

    /**
     * Property key for disabling native code support for generating signatures.
     */
    public static final String MANTA_NO_NATIVE_SIGS_KEY = "manta.disable_native_sigs";

    /**
     * Property key for looking up the timeout value for TCP sockets.
     */
    public static final String MANTA_TCP_SOCKET_TIMEOUT_KEY = "manta.tcp_socket_timeout";

    /**
     * Property key for enabling the checksum verification of uploaded files.
     */
    public static final String MANTA_VERIFY_UPLOADS_KEY = "manta.verify_uploads";

    /**
     * Property key for setting the size of pre-streaming upload buffers.
     */
    public static final String MANTA_UPLOAD_BUFFER_SIZE_KEY = "manta.upload_buffer_size";

    /**
     * Property key for flag indicating when client-side encryption is enabled.
     */
    public static final String MANTA_CLIENT_ENCRYPTION_ENABLED_KEY = "manta.client_encryption";

    /**
     * Property key for setting an identifier for the client-side encryption key used.
     */
    public static final String MANTA_ENCRYPTION_KEY_ID_KEY = "manta.encryption_key_id";

    /**
     * Property key for setting  the name of the algorithm used to encrypt and decrypt.
     */
    public static final String MANTA_ENCRYPTION_ALGORITHM_KEY = "manta.encryption_algorithm";

    /**
     * Property key for flag indicating when downloading unencrypted
     * files is allowed in encryption mode.
     */
    public static final String MANTA_PERMIT_UNENCRYPTED_DOWNLOADS_KEY = "manta.permit_unencrypted_downloads";

    /**
     * Property key for enum specifying if we are in strict ciphertext
     * authentication mode or not.
     */
    public static final String MANTA_ENCRYPTION_AUTHENTICATION_MODE_KEY = "manta.encryption_auth_mode";

    /**
     * Property key for path to the private encryption key on the
     * filesystem (can't be used if private key bytes is not null).
     */
    public static final String MANTA_ENCRYPTION_PRIVATE_KEY_PATH_KEY = "manta.encryption_key_path";

    /**
     * Property key for private encryption key data (can't be used if
     * private key path is not null).
     */
    public static final String MANTA_ENCRYPTION_PRIVATE_KEY_BYTES_KEY = "manta.encryption_key_bytes";

    /**
     * Property key for private encryption key data (can't be used if
     * private key path is not null) and must be passed in base64 encoding.
     */
    public static final String MANTA_ENCRYPTION_PRIVATE_KEY_BYTES_BASE64_KEY = "manta.encryption_key_bytes_base64";

    // I know manually adding them all sucks, but it is the simplest operation
    // for a shared library. We could do all sorts of complicated reflection
    // or annotation processing, but they are error-prone.
    /**
     * List of all properties that we read from configuration.
     */
    public static final String[] ALL_PROPERTIES = {
            MANTA_URL_KEY, MANTA_USER_KEY, MANTA_KEY_ID_KEY,
            MANTA_KEY_PATH_KEY, MANTA_TIMEOUT_KEY, MANTA_RETRIES_KEY,
            MANTA_MAX_CONNS_KEY, MANTA_PRIVATE_KEY_CONTENT_KEY,
            MANTA_PASSWORD_KEY, MANTA_HTTP_BUFFER_SIZE_KEY,
            MANTA_HTTPS_PROTOCOLS_KEY, MANTA_HTTPS_CIPHERS_KEY,
            MANTA_NO_AUTH_KEY, MANTA_NO_NATIVE_SIGS_KEY,
            MANTA_TCP_SOCKET_TIMEOUT_KEY,
            MANTA_VERIFY_UPLOADS_KEY,
            MANTA_UPLOAD_BUFFER_SIZE_KEY,
            MANTA_CLIENT_ENCRYPTION_ENABLED_KEY,
            MANTA_PERMIT_UNENCRYPTED_DOWNLOADS_KEY,
            MANTA_ENCRYPTION_KEY_ID_KEY,
            MANTA_ENCRYPTION_ALGORITHM_KEY,
            MANTA_ENCRYPTION_AUTHENTICATION_MODE_KEY,
            MANTA_ENCRYPTION_PRIVATE_KEY_PATH_KEY,
            MANTA_ENCRYPTION_PRIVATE_KEY_BYTES_KEY,
            MANTA_ENCRYPTION_PRIVATE_KEY_BYTES_BASE64_KEY
    };

    static {
        // Sorts the properties so that we can do a binary search on them if needed
        Arrays.sort(ALL_PROPERTIES);
    }

    /**
     * Internal map used as the source of the configuration bean values.
     */
    private final Map<?, ?> backingMap;

    /**
     * Creates a new instance using the passed {@link Map} implementation as
     * a backing store.
     *
     * @param backingMap Map implementation used for the values of the configuration beans
     */
    public MapConfigContext(final Map<?, ?> backingMap) {
        this.backingMap = backingMap;
    }

    @Override
    public String getMantaURL() {
        return normalizeEmptyAndNullAndDefaultToStringValue(
                MANTA_URL_KEY, MANTA_URL_ENV_KEY);
    }

    @Override
    public String getMantaUser() {
        return normalizeEmptyAndNullAndDefaultToStringValue(
                MANTA_USER_KEY, MANTA_ACCOUNT_ENV_KEY);
    }

    @Override
    public String getMantaKeyId() {
        return normalizeEmptyAndNullAndDefaultToStringValue(
                MANTA_KEY_ID_KEY, MANTA_KEY_ID_ENV_KEY);
    }

    @Override
    public String getMantaKeyPath() {
        return normalizeEmptyAndNullAndDefaultToStringValue(
                MANTA_KEY_PATH_KEY, MANTA_KEY_PATH_ENV_KEY);
    }

    @Override
    public String getPrivateKeyContent() {
        return normalizeEmptyAndNullAndDefaultToStringValue(
                MANTA_PRIVATE_KEY_CONTENT_KEY, MANTA_PRIVATE_KEY_CONTENT_ENV_KEY);
    }

    @Override
    public String getPassword() {
        return normalizeEmptyAndNullAndDefaultToStringValue(
                MANTA_PASSWORD_KEY, MANTA_PASSWORD_ENV_KEY);
    }

    @Override
    public String getMantaHomeDirectory() {
        return ConfigContext.deriveHomeDirectoryFromUser(getMantaUser());
    }

    @Override
    public Integer getTimeout() {
        Integer mapValue = MantaUtils.parseIntegerOrNull(backingMap.get(MANTA_TIMEOUT_KEY));

        if (mapValue != null) {
            return mapValue;
        }

        return MantaUtils.parseIntegerOrNull(backingMap.get(MANTA_TIMEOUT_ENV_KEY));
    }

    @Override
    public Integer getRetries() {
        Integer mapValue = MantaUtils.parseIntegerOrNull(backingMap.get(MANTA_RETRIES_KEY));

        if (mapValue != null) {
            return mapValue;
        }

        return MantaUtils.parseIntegerOrNull(backingMap.get(MANTA_RETRIES_ENV_KEY));
    }

    @Override
    public Integer getMaximumConnections() {
        Integer mapValue = MantaUtils.parseIntegerOrNull(backingMap.get(MANTA_MAX_CONNS_KEY));

        if (mapValue != null) {
            return mapValue;
        }

        return MantaUtils.parseIntegerOrNull(backingMap.get(MANTA_MAX_CONNS_ENV_KEY));
    }

    @Override
    public Integer getHttpBufferSize() {
        Integer mapValue = MantaUtils.parseIntegerOrNull(backingMap.get(MANTA_HTTP_BUFFER_SIZE_KEY));

        if (mapValue != null) {
            return mapValue;
        }

        return MantaUtils.parseIntegerOrNull(backingMap.get(MANTA_HTTP_BUFFER_SIZE_ENV_KEY));
    }

    @Override
    public String getHttpsProtocols() {
        return normalizeEmptyAndNullAndDefaultToStringValue(
                MANTA_HTTPS_PROTOCOLS_KEY, MANTA_HTTPS_PROTOCOLS_ENV_KEY);
    }

    @Override
    public String getHttpsCipherSuites() {
        return normalizeEmptyAndNullAndDefaultToStringValue(
                MANTA_HTTPS_CIPHERS_KEY, MANTA_HTTPS_CIPHERS_ENV_KEY);
    }

    @Override
    public Boolean noAuth() {
        Boolean mapValue = MantaUtils.parseBooleanOrNull(backingMap.get(MANTA_NO_AUTH_KEY));

        if (mapValue != null) {
            return mapValue;
        }

        return MantaUtils.parseBooleanOrNull(backingMap.get(MANTA_NO_AUTH_ENV_KEY));
    }

    @Override
    public Boolean disableNativeSignatures() {
        Boolean mapValue = MantaUtils.parseBooleanOrNull(backingMap.get(MANTA_NO_NATIVE_SIGS_KEY));

        if (mapValue != null) {
            return mapValue;
        }

        return MantaUtils.parseBooleanOrNull(backingMap.get(MANTA_NO_NATIVE_SIGS_ENV_KEY));
    }

    @Override
    public Integer getTcpSocketTimeout() {
        Integer mapValue = MantaUtils.parseIntegerOrNull(backingMap.get(MANTA_TCP_SOCKET_TIMEOUT_KEY));

        if (mapValue != null) {
            return mapValue;
        }

        return MantaUtils.parseIntegerOrNull(backingMap.get(MANTA_TCP_SOCKET_TIMEOUT_ENV_KEY));
    }

    @Override
    public Boolean verifyUploads() {
        Boolean mapValue = MantaUtils.parseBooleanOrNull(backingMap.get(MANTA_VERIFY_UPLOADS_KEY));

        if (mapValue != null) {
            return mapValue;
        }

        return MantaUtils.parseBooleanOrNull(backingMap.get(MANTA_VERIFY_UPLOADS_ENV_KEY));
    }

    @Override
    public Integer getUploadBufferSize() {
        Integer mapValue = MantaUtils.parseIntegerOrNull(backingMap.get(MANTA_UPLOAD_BUFFER_SIZE_KEY));

        if (mapValue != null) {
            return mapValue;
        }

        return MantaUtils.parseIntegerOrNull(backingMap.get(MANTA_UPLOAD_BUFFER_SIZE_ENV_KEY));
    }

    @Override
    public Boolean isClientEncryptionEnabled() {
        Boolean enabled = MantaUtils.parseBooleanOrNull(backingMap.get(MANTA_CLIENT_ENCRYPTION_ENABLED_KEY));

        if (enabled != null) {
            return enabled;
        }

        return MantaUtils.parseBooleanOrNull(backingMap.get(MANTA_CLIENT_ENCRYPTION_ENABLED_ENV_KEY));
    }

    @Override
    public String getEncryptionKeyId() {
        return normalizeEmptyAndNullAndDefaultToStringValue(
                MANTA_ENCRYPTION_KEY_ID_KEY, MANTA_ENCRYPTION_KEY_ID_ENV_KEY);
    }

    @Override
    public String getEncryptionAlgorithm() {
        return normalizeEmptyAndNullAndDefaultToStringValue(
                MANTA_ENCRYPTION_ALGORITHM_KEY, MANTA_ENCRYPTION_ALGORITHM_ENV_KEY);
    }

    @Override
    public Boolean permitUnencryptedDownloads() {
        Boolean permit = MantaUtils.parseBooleanOrNull(backingMap.get(MANTA_PERMIT_UNENCRYPTED_DOWNLOADS_KEY));

        if (permit != null) {
            return permit;
        }

        return MantaUtils.parseBooleanOrNull(backingMap.get(MANTA_CLIENT_ENCRYPTION_ENABLED_ENV_KEY));
    }

    @Override
    public EncryptionAuthenticationMode getEncryptionAuthenticationMode() {
        EncryptionAuthenticationMode authMode = MantaUtils.parseEnumOrNull(
                backingMap.get(MANTA_ENCRYPTION_AUTHENTICATION_MODE_KEY), EncryptionAuthenticationMode.class);

        if (authMode != null) {
            return authMode;
        }

        return MantaUtils.parseEnumOrNull(
                backingMap.get(MANTA_ENCRYPTION_AUTHENTICATION_MODE_ENV_KEY), EncryptionAuthenticationMode.class);
    }

    @Override
    public String getEncryptionPrivateKeyPath() {
        return normalizeEmptyAndNullAndDefaultToStringValue(
                MANTA_ENCRYPTION_PRIVATE_KEY_PATH_KEY,
                MANTA_ENCRYPTION_PRIVATE_KEY_PATH_ENV_KEY);
    }

    @Override
    public byte[] getEncryptionPrivateKeyBytes() {
        String base64 = normalizeEmptyAndNullAndDefaultToStringValue(
                MANTA_ENCRYPTION_PRIVATE_KEY_BYTES_BASE64_KEY,
                MANTA_ENCRYPTION_PRIVATE_KEY_BYTES_BASE64_ENV_KEY);

        Object bytesObj = backingMap.get(MANTA_ENCRYPTION_PRIVATE_KEY_BYTES_KEY);
        final byte[] bytes;

        if (bytesObj instanceof byte[]) {
            bytes = (byte[])bytesObj;
        } else {
            bytes = null;
        }

        if (bytes != null && base64 != null) {
            String msg = "You can't set a base64 private key value AND a byte "
                         + "array value at the same time";
            throw new IllegalArgumentException(msg);
        }

        if (base64 != null) {
            return Base64.getDecoder().decode(base64);
        }

        return bytes;
    }

    /**
     * Allows the caller to perform a put operation on the backing map of the
     * context. This is typically used by other {@link ConfigContext}
     * implementations that need to cobble together multiple map values.
     *
     * This method is scoped to default because no other packages should be
     * using it.
     *
     * @param key configuration key
     * @param value configuration value
     * @return return value of the put() operation from the backing map
     */
    Object put(final String key, final String value) {
        if (key == null) {
            throw new IllegalArgumentException("Config key can't be null");
        }

        if (key.isEmpty()) {
            throw new IllegalArgumentException("Config key can't be blank");
        }

        // Java generics can be stupid
        @SuppressWarnings("unchecked")
        Map<Object, Object> map = (Map<Object, Object>)this.backingMap;
        return map.put(key, value);
    }

    /**
     * Normalizes a value pulled from the backingMap.
     * @param keys key to use to pull value from backing map
     * @return null on empty string or null, otherwise value from backing map
     */
    private String normalizeEmptyAndNullAndDefaultToStringValue(final Object... keys) {
        for (Object k : keys) {
            String value = MantaUtils.toStringEmptyToNull(backingMap.get(k));
            if (value == null) {
                continue;
            }

            return value;
        }

        // We couldn't find any values
        return null;
    }

    @Override
    public String toString() {
        return ConfigContext.toString(this);
    }
}
