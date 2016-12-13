/*
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.config;

import com.joyent.manta.util.MantaUtils;

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
     * Environment variable for setting HttpTransport implementation.
     */
    public static final String MANTA_HTTP_TRANSPORT_ENV_KEY = "MANTA_HTTP_TRANSPORT";

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
     * Environment variable for looking up the time in milliseconds to cache
     * HTTP signature headers.
     */
    public static final String MANTA_SIGS_CACHE_TTL_ENV_KEY = "MANTA_SIGS_CACHE_TTL";

    /**
     * Environment variable for flag indicating when client-side encryption is enabled.
     */
    public static final String MANTA_CLIENT_ENCRYPTION_ENABLED_ENV_KEY = "MANTA_CLIENT_ENCRYPTION";

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
            MANTA_PASSWORD_ENV_KEY, MANTA_HTTP_TRANSPORT_ENV_KEY,
            MANTA_HTTPS_PROTOCOLS_ENV_KEY, MANTA_HTTPS_CIPHERS_ENV_KEY,
            MANTA_NO_AUTH_ENV_KEY, MANTA_NO_NATIVE_SIGS_ENV_KEY,
            MANTA_SIGS_CACHE_TTL_ENV_KEY,
            MANTA_CLIENT_ENCRYPTION_ENABLED_ENV_KEY,
            MANTA_PERMIT_UNENCRYPTED_DOWNLOADS_ENV_KEY,
            MANTA_ENCRYPTION_AUTHENTICATION_MODE_ENV_KEY,
            MANTA_ENCRYPTION_PRIVATE_KEY_PATH_ENV_KEY,
            MANTA_ENCRYPTION_PRIVATE_KEY_BYTES_BASE64_ENV_KEY
    };

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
    @Deprecated
    public String getHttpTransport() {
        return getEnv(MANTA_HTTP_TRANSPORT_ENV_KEY);
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
    public Integer getSignatureCacheTTL() {
        String ttl = getEnv(MANTA_SIGS_CACHE_TTL_ENV_KEY);

        return MantaUtils.parseIntegerOrNull(ttl);
    }

    @Override
    public Boolean isClientEncryptionEnabled() {
        String enabled = getEnv(MANTA_CLIENT_ENCRYPTION_ENABLED_ENV_KEY);

        return MantaUtils.parseBooleanOrNull(enabled);
    }

    @Override
    public Boolean permitUnencryptedDownloads() {
        String permit = getEnv(MANTA_PERMIT_UNENCRYPTED_DOWNLOADS_ENV_KEY);

        return MantaUtils.parseBooleanOrNull(permit);
    }

    @Override
    public EncryptionObjectAuthenticationMode getEncryptionAuthenticationMode() {
        String enumValue = getEnv(MANTA_ENCRYPTION_AUTHENTICATION_MODE_ENV_KEY);

        return MantaUtils.parseEnumOrNull(enumValue, EncryptionObjectAuthenticationMode.class);
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
