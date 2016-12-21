package com.joyent.manta.config;

import com.joyent.manta.util.MantaUtils;
import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.util.encoders.Base64;

import java.util.Objects;

/**
 * Interface defining the setters for mutable {@link ConfigContext} objects.
 *
 * @param <T> Type of class implemented so we can have builder style setters.
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public interface SettableConfigContext<T> extends ConfigContext {
    /**
     * Sets the Manta service endpoint.
     * @param mantaURL Manta service endpoint
     * @return the current instance of {@link T}
     */
    T setMantaURL(String mantaURL);

    /**
     * Sets the account associated with the Manta service.
     * @param mantaUser Manta user account
     * @return the current instance of {@link T}
     */
    T setMantaUser(String mantaUser);

    /**
     * Sets the RSA key fingerprint of the private key used to access Manta.
     * @param mantaKeyId RSA key fingerprint
     * @return the current instance of {@link T}
     */
    T setMantaKeyId(String mantaKeyId);

    /**
     * Sets the path on the filesystem to the private RSA key used to access Manta.
     * @param mantaKeyPath path on the filesystem
     * @return the current instance of {@link T}
     */
    T setMantaKeyPath(String mantaKeyPath);

    /**
     * Sets the general connection timeout for the Manta service.
     * @param timeout timeout in milliseconds
     * @return the current instance of {@link T}
     */
    T setTimeout(Integer timeout);

    /**
     * Sets the number of times to retry failed HTTP requests.
     * @param retries number of times to retry
     * @return the current instance of {@link T}
     */
    T setRetries(Integer retries);

    /**
     * Sets the maximum number of open connections to the Manta API.
     * @param maxConns number of connections greater than zero
     * @return the current instance of {@link T}
     */
    T setMaximumConnections(Integer maxConns);

    /**
     * Sets the private key content used to authenticate. This can't be set if
     * you already have a private key path specified.
     * @param privateKeyContent contents of private key in plain text
     * @return the current instance of {@link T}
     */
    T setPrivateKeyContent(String privateKeyContent);

    /**
     * Sets the password used for the private key. This is optional and not
     * typically used.
     * @param password password to set
     * @return the current instance of {@link T}
     */
    T setPassword(String password);

    /**
     * This method is no longer used.
     *
     * @param httpTransport Typically 'ApacheHttpTransport' or 'NetHttpTransport'
     * @return the current instance of {@link T}
     */
    T setHttpTransport(String httpTransport);

    /**
     * Set the supported TLS protocols.
     *
     * @param httpsProtocols comma delimited list of TLS protocols
     * @return the current instance of {@link T}
     */
    T setHttpsProtocols(String httpsProtocols);

    /**
     * Set the supported TLS ciphers.
     *
     * @param httpsCiphers comma delimited list of TLS ciphers
     * @return the current instance of {@link T}
     */
    T setHttpsCiphers(String httpsCiphers);

    /**
     * Change the state of whether or not HTTP signatures are sent to the Manta API.
     *
     * @param noAuth true to disable HTTP signatures
     * @return the current instance of {@link T}
     */
    T setNoAuth(Boolean noAuth);

    /**
     * Change the state of whether or not HTTP signatures are using native code
     * to generate the cryptographic signatures.
     *
     * @param disableNativeSignatures true to disable
     * @return the current instance of {@link T}
     */
    T setDisableNativeSignatures(Boolean disableNativeSignatures);

    /**
     * Sets the time in milliseconds to cache HTTP signature headers.
     *
     * @param signatureCacheTTL time in milliseconds to cache HTTP signature headers
     * @return the current instance of {@link T}
     */
    T setSignatureCacheTTL(Integer signatureCacheTTL);

    /**
     * Sets the maximum number of open connections to the Manta API.
     *
     * @param maxConnections maximum number of open connections to open
     * @return the current instance of {@link T}
     */
    T setMaxConnections(Integer maxConnections);

    /**
     * Sets flag indicating when client-side encryption is enabled.
     *
     * @param clientEncryptionEnabled true if client-side encryption is enabled
     * @return the current instance of {@link T}
     */
    T setClientEncryptionEnabled(Boolean clientEncryptionEnabled);

    /**
     * Sets flag indicating when downloading unencrypted files is allowed in
     * encryption mode.
     *
     * @param permitUnencryptedDownloads true if downloading unencrypted data is permitted when in encrypted mode
     * @return the current instance of {@link T}
     */
    T setPermitUnencryptedDownloads(Boolean permitUnencryptedDownloads);

    /**
     * Sets enum specifying if we are in strict ciphertext authentication mode
     * or not.
     *
     * @param encryptionAuthenticationMode enum of authentication mode
     * @return the current instance of {@link T}
     */
    T setEncryptionAuthenticationMode(
            EncryptionObjectAuthenticationMode encryptionAuthenticationMode);

    /**
     * Sets the path to the private encryption key on the filesystem (can't be
     * used if private key bytes is not null).
     *
     * @param encryptionPrivateKeyPath path to private encryption key of file system
     * @return the current instance of {@link T}
     */
    T setEncryptionPrivateKeyPath(String encryptionPrivateKeyPath);

    /**
     * Sets the private encryption key data in memory (can't be used if private
     * key path is not null).
     *
     * @param encryptionPrivateKeyBytes byte array containing private key data
     * @return the current instance of {@link T}
     */
    T setEncryptionPrivateKeyBytes(byte[] encryptionPrivateKeyBytes);

    /**
     * Utility method for setting a {@link SettableConfigContext} values via
     * String parameters. Note, this method will set null for values when the
     * inputs are invalid for integer or boolean types. For other types it
     * will just not set anything.
     *
     * @param name string key to set via
     * @param value value to set to context
     * @param config config value to set
     */
    static void setAttributeFromContext(final String name,
                                        final Object value,
                                        final SettableConfigContext<?> config) {
        switch (name) {
            case MapConfigContext.MANTA_URL_KEY:
            case EnvVarConfigContext.MANTA_URL_ENV_KEY:
                config.setMantaURL(Objects.toString(value));
                break;
            case MapConfigContext.MANTA_USER_KEY:
            case EnvVarConfigContext.MANTA_ACCOUNT_ENV_KEY:
                config.setMantaUser(Objects.toString(value));
                break;
            case MapConfigContext.MANTA_KEY_ID_KEY:
            case EnvVarConfigContext.MANTA_KEY_ID_ENV_KEY:
                config.setMantaKeyId(Objects.toString(value));
                break;
            case MapConfigContext.MANTA_KEY_PATH_KEY:
            case EnvVarConfigContext.MANTA_KEY_PATH_ENV_KEY:
                config.setMantaKeyPath(Objects.toString(value));
                break;
            case MapConfigContext.MANTA_TIMEOUT_KEY:
            case EnvVarConfigContext.MANTA_TIMEOUT_ENV_KEY:
                config.setTimeout(MantaUtils.parseIntegerOrNull(value));
                break;
            case MapConfigContext.MANTA_RETRIES_KEY:
            case EnvVarConfigContext.MANTA_RETRIES_ENV_KEY:
                config.setRetries(MantaUtils.parseIntegerOrNull(value));
                break;
            case MapConfigContext.MANTA_MAX_CONNS_KEY:
            case EnvVarConfigContext.MANTA_MAX_CONNS_ENV_KEY:
                config.setMaximumConnections(MantaUtils.parseIntegerOrNull(value));
                break;
            case MapConfigContext.MANTA_PRIVATE_KEY_CONTENT_KEY:
            case EnvVarConfigContext.MANTA_PRIVATE_KEY_CONTENT_ENV_KEY:
                config.setPrivateKeyContent(Objects.toString(value));
                break;
            case MapConfigContext.MANTA_PASSWORD_KEY:
            case EnvVarConfigContext.MANTA_PASSWORD_ENV_KEY:
                config.setPassword(Objects.toString(value));
                break;
            case MapConfigContext.MANTA_HTTP_TRANSPORT_KEY:
            case EnvVarConfigContext.MANTA_HTTP_TRANSPORT_ENV_KEY:
                config.setHttpTransport(Objects.toString(value));
                break;
            case MapConfigContext.MANTA_HTTPS_PROTOCOLS_KEY:
            case EnvVarConfigContext.MANTA_HTTPS_PROTOCOLS_ENV_KEY:
                config.setHttpsProtocols(Objects.toString(value));
                break;
            case MapConfigContext.MANTA_HTTPS_CIPHERS_KEY:
            case EnvVarConfigContext.MANTA_HTTPS_CIPHERS_ENV_KEY:
                config.setHttpsCiphers(Objects.toString(value));
                break;
            case MapConfigContext.MANTA_NO_AUTH_KEY:
            case EnvVarConfigContext.MANTA_NO_AUTH_ENV_KEY:
                config.setNoAuth(MantaUtils.parseBooleanOrNull(value));
                break;
            case MapConfigContext.MANTA_NO_NATIVE_SIGS_KEY:
            case EnvVarConfigContext.MANTA_NO_NATIVE_SIGS_ENV_KEY:
                config.disableNativeSignatures();
                break;
            case MapConfigContext.MANTA_SIGS_CACHE_TTL_KEY:
            case EnvVarConfigContext.MANTA_SIGS_CACHE_TTL_ENV_KEY:
                config.setSignatureCacheTTL(MantaUtils.parseIntegerOrNull(value));
                break;
            case MapConfigContext.MANTA_CLIENT_ENCRYPTION_ENABLED_KEY:
            case EnvVarConfigContext.MANTA_CLIENT_ENCRYPTION_ENABLED_ENV_KEY:
                config.setClientEncryptionEnabled(MantaUtils.parseBooleanOrNull(value));
                break;
            case MapConfigContext.MANTA_PERMIT_UNENCRYPTED_DOWNLOADS_KEY:
            case EnvVarConfigContext.MANTA_PERMIT_UNENCRYPTED_DOWNLOADS_ENV_KEY:
                config.setPermitUnencryptedDownloads(MantaUtils.parseBooleanOrNull(value));
                break;
            case MapConfigContext.MANTA_ENCRYPTION_AUTHENTICATION_MODE_KEY:
            case EnvVarConfigContext.MANTA_ENCRYPTION_AUTHENTICATION_MODE_ENV_KEY:
                String stringVal = Objects.toString(value);
                if (StringUtils.isBlank(stringVal)) {
                    return;
                }

                try {
                    config.setEncryptionAuthenticationMode(
                            EncryptionObjectAuthenticationMode.valueOf(stringVal));
                } catch (IllegalArgumentException e) {
                    // error parsing enum value, so we just exit the function
                    return;
                }

                break;
            case MapConfigContext.MANTA_ENCRYPTION_PRIVATE_KEY_PATH_KEY:
            case EnvVarConfigContext.MANTA_ENCRYPTION_PRIVATE_KEY_PATH_ENV_KEY:
                config.setEncryptionPrivateKeyPath(Objects.toString(value));
                break;
            case MapConfigContext.MANTA_ENCRYPTION_PRIVATE_KEY_BYTES_KEY:
                if (value instanceof byte[]) {
                    config.setEncryptionPrivateKeyBytes((byte[])value);
                }
                break;
            case MapConfigContext.MANTA_ENCRYPTION_PRIVATE_KEY_BYTES_BASE64_KEY:
            case EnvVarConfigContext.MANTA_ENCRYPTION_PRIVATE_KEY_BYTES_BASE64_ENV_KEY:
                if (value instanceof String) {
                    String base64 = (String)value;
                    if (StringUtils.isEmpty(base64)) {
                        return;
                    }

                    config.setEncryptionPrivateKeyBytes(Base64.decode(base64));
                }
                break;
            default:
                break;
        }
    }
}
