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

/**
 * An implementation of {@link ConfigContext} that reads its configuration
 * from expected environment variables.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class EnvVarConfigContext implements ConfigContext {
    /**
     * Environment variable for looking up a Manta URL.
     */
    public static final String MANTA_URL_ENV_KEY = "MANTA_URL";

    /**
     * Environment variable for looking up a Manta account.
     */
    public static final String MANTA_ACCOUNT_ENV_KEY = "MANTA_USER";

    /**
     * Environment variable for looking up a RSA fingerprint.
     */
    public static final String MANTA_KEY_ID_ENV_KEY = "MANTA_KEY_ID";

    /**
     * Environment variable for looking up a RSA private key path.
     */
    public static final String MANTA_KEY_PATH_ENV_KEY = "MANTA_KEY_PATH";

    /**
     * Environment variable for looking up a Manta timeout.
     */
    public static final String MANTA_TIMEOUT_ENV_KEY = "MANTA_TIMEOUT";

    /**
     * Environment variable for number of times to retry failed requests.
     */
    public static final String MANTA_RETRIES_ENV_KEY = "MANTA_HTTP_RETRIES";

    /**
     * Environment variable for the maximum number of open connections to the Manta API.
     */
    public static final String MANTA_MAX_CONNS_ENV_KEY = "MANTA_MAX_CONNS";

    /**
     * Environment variable for looking up Manta private key content.
     */
    public static final String MANTA_PRIVATE_KEY_CONTENT_ENV_KEY = "MANTA_KEY_CONTENT";

    /**
     * Environment variable for looking up Manta password.
     */
    public static final String MANTA_PASSWORD_ENV_KEY = "MANTA_PASSWORD";

    /**
     * Environment variable for setting HTTP buffer size.
     */
    public static final String MANTA_HTTP_BUFFER_SIZE_ENV_KEY = "MANTA_HTTP_BUFFER_SIZE";

    /**
     * Environment variable for setting TLS protocols.
     */
    public static final String MANTA_HTTPS_PROTOCOLS_ENV_KEY = "MANTA_HTTPS_PROTOCOLS";

    /**
     * Environment variable for setting TLS ciphers.
     */
    public static final String MANTA_HTTPS_CIPHERS_ENV_KEY = "MANTA_HTTPS_CIPHERS";

    /**
     * Environment variable for disabling HTTP signatures.
     */
    public static final String MANTA_NO_AUTH_ENV_KEY = "MANTA_NO_AUTH";

    /**
     * Environment variable for disabling native code support for generating signatures.
     */
    public static final String MANTA_NO_NATIVE_SIGS_ENV_KEY = "MANTA_NO_NATIVE_SIGS";

    /**
     * Environment variable for looking up the timeout value for TCP sockets.
     */
    public static final String MANTA_TCP_SOCKET_TIMEOUT_ENV_KEY = "MANTA_TCP_SOCKET_TIMEOUT";

    /**
     * Environment variable for enabling the checksum verification of uploaded files.
     */
    public static final String MANTA_VERIFY_UPLOADS_ENV_KEY = "MANTA_VERIFY_UPLOADS";

    /**
     * Environment variable for setting the size of pre-streaming upload buffers.
     */
    public static final String MANTA_UPLOAD_BUFFER_SIZE_ENV_KEY = "MANTA_UPLOAD_BUFFER_SIZE";

    /**
     * Environment variable for flag indicating when client-side encryption is enabled.
     */
    public static final String MANTA_CLIENT_ENCRYPTION_ENABLED_ENV_KEY = "MANTA_CLIENT_ENCRYPTION";

    /**
     * Environment variable for setting an identifier for the client-side encryption key used.
     */
    public static final String MANTA_ENCRYPTION_KEY_ID_ENV_KEY = "MANTA_CLIENT_ENCRYPTION_KEY_ID";

    /**
     * Environment variable for setting the name of the algorithm used to encrypt and decrypt.
     */
    public static final String MANTA_ENCRYPTION_ALGORITHM_ENV_KEY = "MANTA_ENCRYPTION_ALGORITHM";

    /**
     * Environment variable for flag indicating when downloading unencrypted
     * files is allowed in encryption mode.
     */
    public static final String MANTA_PERMIT_UNENCRYPTED_DOWNLOADS_ENV_KEY = "MANTA_UNENCRYPTED_DOWNLOADS";

    /**
     * Environment variable for enum specifying if we are in strict ciphertext
     * authentication mode or not.
     */
    public static final String MANTA_ENCRYPTION_AUTHENTICATION_MODE_ENV_KEY = "MANTA_ENCRYPTION_AUTH_MODE";

    /**
     * Environment variable for path to the private encryption key on the
     * filesystem (can't be used if private key bytes is not null).
     */
    public static final String MANTA_ENCRYPTION_PRIVATE_KEY_PATH_ENV_KEY = "MANTA_ENCRYPTION_KEY_PATH";

    /**
     * Environment variable for private encryption key data (can't be used if
     * private key path is not null) and must be passed in base64 encoding.
     */
    public static final String MANTA_ENCRYPTION_PRIVATE_KEY_BYTES_BASE64_ENV_KEY = "MANTA_ENCRYPTION_KEY_BYTES";

    /**
     * Array of all environment variable names used.
     */
    public static final String[] ALL_PROPERTIES = {
            MANTA_URL_ENV_KEY, MANTA_ACCOUNT_ENV_KEY, MANTA_KEY_ID_ENV_KEY,
            MANTA_KEY_PATH_ENV_KEY, MANTA_TIMEOUT_ENV_KEY, MANTA_RETRIES_ENV_KEY,
            MANTA_MAX_CONNS_ENV_KEY, MANTA_PRIVATE_KEY_CONTENT_ENV_KEY,
            MANTA_PASSWORD_ENV_KEY, MANTA_HTTP_BUFFER_SIZE_ENV_KEY,
            MANTA_HTTPS_PROTOCOLS_ENV_KEY, MANTA_HTTPS_CIPHERS_ENV_KEY,
            MANTA_NO_AUTH_ENV_KEY, MANTA_NO_NATIVE_SIGS_ENV_KEY,
            MANTA_TCP_SOCKET_TIMEOUT_ENV_KEY,
            MANTA_VERIFY_UPLOADS_ENV_KEY,
            MANTA_UPLOAD_BUFFER_SIZE_ENV_KEY,
            MANTA_CLIENT_ENCRYPTION_ENABLED_ENV_KEY,
            MANTA_ENCRYPTION_KEY_ID_ENV_KEY,
            MANTA_PERMIT_UNENCRYPTED_DOWNLOADS_ENV_KEY,
            MANTA_ENCRYPTION_AUTHENTICATION_MODE_ENV_KEY,
            MANTA_ENCRYPTION_PRIVATE_KEY_PATH_ENV_KEY,
            MANTA_ENCRYPTION_PRIVATE_KEY_BYTES_BASE64_ENV_KEY
    };

    static {
        // Sorts the properties so that we can do a binary search on them if needed
        Arrays.sort(ALL_PROPERTIES);
    }

    /**
     * Creates a new instance that provides configuration beans via the
     * values specified in environment variables.
     */
    public EnvVarConfigContext() {
    }

    /**
     * Get the value of an environment variable where an empty string is
     * converted to null.
     *
     * @param var Environment variable name
     * @return value of environment variable
     */
    private static String getEnv(final String var) {
        return MantaUtils.toStringEmptyToNull(System.getenv(var));
    }

    @Override
    public String getMantaURL() {
        return getEnv(MANTA_URL_ENV_KEY);
    }

    @Override
    public String getMantaUser() {
        return getEnv(MANTA_ACCOUNT_ENV_KEY);
    }

    @Override
    public String getMantaKeyId() {
        return getEnv(MANTA_KEY_ID_ENV_KEY);
    }

    @Override
    public String getMantaKeyPath() {
        return getEnv(MANTA_KEY_PATH_ENV_KEY);
    }

    @Override
    public String getPrivateKeyContent() {
        return getEnv(MANTA_PRIVATE_KEY_CONTENT_ENV_KEY);
    }

    @Override
    public String getPassword() {
        return getEnv(MANTA_PASSWORD_ENV_KEY);
    }

    @Override
    public String getMantaHomeDirectory() {
        return ConfigContext.deriveHomeDirectoryFromUser(getMantaUser());
    }

    @Override
    public Integer getTimeout() {
        String timeoutString = getEnv(MANTA_TIMEOUT_ENV_KEY);
        return MantaUtils.parseIntegerOrNull(timeoutString);
    }

    @Override
    public Integer getRetries() {
        String retriesString = getEnv(MANTA_RETRIES_ENV_KEY);
        return MantaUtils.parseIntegerOrNull(retriesString);
    }

    @Override
    public Integer getMaximumConnections() {
        String maxConnsString = getEnv(MANTA_MAX_CONNS_ENV_KEY);
        return MantaUtils.parseIntegerOrNull(maxConnsString);
    }

    @Override
    public Integer getHttpBufferSize() {
        String sizeString = getEnv(MANTA_HTTP_BUFFER_SIZE_ENV_KEY);
        return MantaUtils.parseIntegerOrNull(sizeString);
    }

    @Override
    public String getHttpsProtocols() {
        return getEnv(MANTA_HTTPS_PROTOCOLS_ENV_KEY);
    }

    @Override
    public String getHttpsCipherSuites() {
        return getEnv(MANTA_HTTPS_CIPHERS_ENV_KEY);
    }

    @Override
    public Boolean noAuth() {
        String noAuthString = getEnv(MANTA_NO_AUTH_ENV_KEY);
        return MantaUtils.parseBooleanOrNull(noAuthString);
    }

    @Override
    public Boolean disableNativeSignatures() {
        String disableNativeString = getEnv(MANTA_NO_NATIVE_SIGS_ENV_KEY);
        return MantaUtils.parseBooleanOrNull(disableNativeString);
    }

    @Override
    public Integer getTcpSocketTimeout() {
        String timeoutString = getEnv(MANTA_TCP_SOCKET_TIMEOUT_ENV_KEY);

        return MantaUtils.parseIntegerOrNull(timeoutString);
    }

    @Override
    public Boolean verifyUploads() {
        String verify = getEnv(MANTA_VERIFY_UPLOADS_ENV_KEY);
        return MantaUtils.parseBooleanOrNull(verify);
    }

    @Override
    public Integer getUploadBufferSize() {
        String buffString = getEnv(MANTA_UPLOAD_BUFFER_SIZE_ENV_KEY);

        return MantaUtils.parseIntegerOrNull(buffString);
    }

    @Override
    public Boolean isClientEncryptionEnabled() {
        String enabled = getEnv(MANTA_CLIENT_ENCRYPTION_ENABLED_ENV_KEY);

        return MantaUtils.parseBooleanOrNull(enabled);
    }

    @Override
    public String getEncryptionKeyId() {
        return getEnv(MANTA_ENCRYPTION_KEY_ID_ENV_KEY);
    }

    @Override
    public String getEncryptionAlgorithm() {
        return getEnv(MANTA_ENCRYPTION_ALGORITHM_ENV_KEY);
    }

    @Override
    public Boolean permitUnencryptedDownloads() {
        String permit = getEnv(MANTA_PERMIT_UNENCRYPTED_DOWNLOADS_ENV_KEY);

        return MantaUtils.parseBooleanOrNull(permit);
    }

    @Override
    public EncryptionAuthenticationMode getEncryptionAuthenticationMode() {
        String enumValue = getEnv(MANTA_ENCRYPTION_AUTHENTICATION_MODE_ENV_KEY);

        return MantaUtils.parseEnumOrNull(enumValue, EncryptionAuthenticationMode.class);
    }

    @Override
    public String getEncryptionPrivateKeyPath() {
        return getEnv(MANTA_ENCRYPTION_PRIVATE_KEY_PATH_ENV_KEY);
    }

    @Override
    public byte[] getEncryptionPrivateKeyBytes() {
        String base64 = getEnv(MANTA_ENCRYPTION_PRIVATE_KEY_BYTES_BASE64_ENV_KEY);

        if (base64 == null) {
            return null;
        }

        return Base64.getDecoder().decode(base64);
    }

    @Override
    public String toString() {
        return ConfigContext.toString(this);
    }
}
