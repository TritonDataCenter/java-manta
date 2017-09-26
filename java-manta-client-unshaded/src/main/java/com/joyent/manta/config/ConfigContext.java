/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.config;

import com.joyent.manta.client.MBeanable;
import com.joyent.manta.client.MantaMBeanSupervisor;
import com.joyent.manta.client.crypto.SupportedCiphersLookupMap;
import com.joyent.manta.exception.ConfigurationException;
import com.joyent.manta.util.MantaUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Interface representing the configuration properties needed to configure a
 * {@link com.joyent.manta.client.MantaClient}.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public interface ConfigContext extends MBeanable {
    /**
     * @return Manta service endpoint.
     */
    String getMantaURL();

    /**
     * @return account associated with the Manta service.
     */
    String getMantaUser();

    /**
     * @return RSA key fingerprint of the private key used to access Manta.
     */
    String getMantaKeyId();

    /**
     * @return Path on the filesystem to the private RSA key used to access Manta.
     */
    String getMantaKeyPath();

    /**
     * @return private key content. This can't be set if the MantaKeyPath is set.
     */
    String getPrivateKeyContent();

    /**
     * @return password for private key. This is optional and typically not set.
     */
    String getPassword();

    /**
     * @return General connection timeout for the Manta service.
     */
    Integer getTimeout();

    /**
     * @return String of home directory based on Manta username.
     */
    String getMantaHomeDirectory();

    /**
     * @return Number of HTTP retries to perform on failure.
     */
    Integer getRetries();

    /**
     * @return the maximum number of open connections to the Manta API
     */
    Integer getMaximumConnections();

    /**
     * @return size of buffer in bytes to use to buffer streams of HTTP data
     */
    Integer getHttpBufferSize();

    /**
     * @return a comma delimited list of HTTPS protocols
     */
    String getHttpsProtocols();

    /**
     * @return a comma delimited list of HTTPS cipher suites in order of preference
     */
    String getHttpsCipherSuites();

    /**
     * @return true when we disable sending HTTP signatures
     */
    Boolean noAuth();

    /**
     * @return true when we disable using native code to generate HTTP signatures
     */
    Boolean disableNativeSignatures();

    /**
     * @see java.net.SocketOptions#SO_TIMEOUT
     * @return time in milliseconds to wait to see if a TCP socket has timed out
     */
    Integer getTcpSocketTimeout();

    /**
     * @return true when we verify the uploaded file's checksum against the
     *         server's checksum (MD5)
     */
    Boolean verifyUploads();

    /**
     * @return number of bytes to read into memory for a streaming upload before
     *         deciding if we want to load it in memory before send it
     */
    Integer getUploadBufferSize();

    /**
     * @return true when client-side encryption is enabled.
     */
    Boolean isClientEncryptionEnabled();

    /**
     * @return true when downloading unencrypted files is allowed in encryption mode
     */
    Boolean permitUnencryptedDownloads();

    /**
     * @return specifies if we are in strict ciphertext authentication mode or not
     */
    EncryptionAuthenticationMode getEncryptionAuthenticationMode();

    /**
     * A plain-text identifier for the encryption key used. It doesn't contain
     * whitespace and is encoded in US-ASCII. The value of this setting has
     * no current functional impact.
     *
     * @return the unique identifier of the key used for encryption
     */
    String getEncryptionKeyId();

    /**
     * Gets the algorithm name in the format of <code>cipher/mode/padding state</code>.
     *
     * @return the name of the algorithm used to encrypt and decrypt
     */
    String getEncryptionAlgorithm();

    /**
     * @return path to the private encryption key on the filesystem (can't be used if private key bytes is not null)
     */
    String getEncryptionPrivateKeyPath();

    /**
     * @return private encryption key data (can't be used if private key path is not null)
     */
    byte[] getEncryptionPrivateKeyBytes();

    /** {@inheritDoc} */
    @Override
    default void createExposedMBean(final MantaMBeanSupervisor beanSupervisor) {
        Validate.notNull(beanSupervisor, "MantaMBeanSupervisor must not be null");
        beanSupervisor.expose(new ConfigContextMBean(this));
    }

    /**
     * Extracts the home directory based on the Manta account name.
     *
     * @param mantaUser user associated with account
     * @return the root account holder name prefixed with a slash or null
     *         upon null mantaUser
     */
    static String deriveHomeDirectoryFromUser(final String mantaUser) {
        if (mantaUser == null) {
            return null;
        }

        final String[] accountParts = MantaUtils.parseAccount(mantaUser);
        return String.format("/%s", accountParts[0]);
    }

    /**
     * Utility method for generating to string values for all {@link ConfigContext}
     * implementations.
     *
     * @param context Context to generate String value from
     * @return string value of context
     */
    static String toString(final ConfigContext context) {
        final StringBuilder sb = new StringBuilder("BaseChainedConfigContext{");
        sb.append("mantaURL='").append(context.getMantaURL()).append('\'');
        sb.append(", user='").append(context.getMantaUser()).append('\'');
        sb.append(", mantaKeyId='").append(context.getMantaKeyId()).append('\'');
        sb.append(", mantaKeyPath='").append(context.getMantaKeyPath()).append('\'');
        sb.append(", timeout=").append(context.getTimeout());
        sb.append(", retries=").append(context.getRetries());
        sb.append(", maxConnections=").append(context.getMaximumConnections());
        sb.append(", httpBufferSize='").append(context.getHttpBufferSize()).append('\'');
        sb.append(", httpsProtocols='").append(context.getHttpsProtocols()).append('\'');
        sb.append(", httpsCiphers='").append(context.getHttpsCipherSuites()).append('\'');
        sb.append(", noAuth=").append(context.noAuth());
        sb.append(", disableNativeSignatures=").append(context.disableNativeSignatures());
        sb.append(", tcpSocketTimeout=").append(context.getTcpSocketTimeout());
        sb.append(", verifyUploads=").append(context.verifyUploads());
        sb.append(", uploadBufferSize=").append(context.getUploadBufferSize());
        sb.append(", clientEncryptionEnabled=").append(context.isClientEncryptionEnabled());
        sb.append(", permitUnencryptedDownloads=").append(context.permitUnencryptedDownloads());
        sb.append(", encryptionAuthenticationMode=").append(context.getEncryptionAuthenticationMode());
        sb.append(", encryptionKeyId=").append(context.getEncryptionKeyId());
        sb.append(", encryptionAlgorithm=").append(context.getEncryptionAlgorithm());
        sb.append(", encryptionPrivateKeyPath=").append(context.getEncryptionPrivateKeyPath());

        if (context.getEncryptionPrivateKeyBytes() == null) {
            sb.append(", encryptionPrivateKeyBytesLength=").append("null object");
        } else {
            sb.append(", encryptionPrivateKeyBytesLength=").append(context.getEncryptionPrivateKeyBytes().length);
        }

        sb.append('}');
        return sb.toString();
    }

    /**
     * Utility method for validating that the configuration has been instantiated
     * with valid settings.
     *
     * @param config configuration to test
     * @throws ConfigurationException thrown when validation fails
     */
    static void validate(final ConfigContext config) {
        List<String> failureMessages = new ArrayList<>();

        if (StringUtils.isBlank(config.getMantaUser())) {
            failureMessages.add("Manta account name must be specified");
        }

        if (StringUtils.isBlank(config.getMantaURL())) {
            failureMessages.add("Manta URL must be specified");
        } else {
            try {
                new URI(config.getMantaURL());
            } catch (URISyntaxException e) {
                final String msg = String.format("%s - invalid Manta URL: %s",
                        e.getMessage(), config.getMantaURL());
                failureMessages.add(msg);
            }
        }

        if (config.getTimeout() != null && config.getTimeout() < 0) {
            failureMessages.add("Manta timeout must be 0 or greater");
        }

        if (config.noAuth() != null && !config.noAuth()) {
            if (config.getMantaKeyId() == null) {
                failureMessages.add("Manta key id must be specified");
            }
        }

        if (config.getMantaKeyPath() == null && config.getPrivateKeyContent() == null) {
            failureMessages.add("Either the Manta key path must be set or the private "
                    + "key content must be set. Both can't be null.");
        }

        if (config.getMantaKeyPath() != null && StringUtils.isBlank(config.getMantaKeyPath())) {
            failureMessages.add("Manta key path must not be blank");
        }

        if (config.getPrivateKeyContent() != null && StringUtils.isBlank(config.getPrivateKeyContent())) {
            failureMessages.add("Manta private key content must not be blank");
        }

        if (BooleanUtils.isTrue(config.isClientEncryptionEnabled())) {
            encryptionSettings(config, failureMessages);
        }

        if (!failureMessages.isEmpty()) {
            String messages = StringUtils.join(failureMessages, System.lineSeparator());
            ConfigurationException e = new ConfigurationException(
                    "Errors when loading Manta SDK configuration:"
                    + System.lineSeparator() + messages);

            // We don't dump all of the configuration settings, just the important ones

            e.setContextValue(MapConfigContext.MANTA_URL_KEY, config.getMantaURL());
            e.setContextValue(MapConfigContext.MANTA_USER_KEY, config.getMantaUser());
            e.setContextValue(MapConfigContext.MANTA_KEY_ID_KEY, config.getMantaKeyId());
            e.setContextValue(MapConfigContext.MANTA_NO_AUTH_KEY, config.noAuth());
            e.setContextValue(MapConfigContext.MANTA_KEY_PATH_KEY, config.getMantaKeyPath());

            final String redactedPrivateKeyContent;
            if (config.getPrivateKeyContent() == null) {
                redactedPrivateKeyContent = "null";
            } else {
                redactedPrivateKeyContent = "non-null";
            }

            e.setContextValue(MapConfigContext.MANTA_PRIVATE_KEY_CONTENT_KEY, redactedPrivateKeyContent);
            e.setContextValue(MapConfigContext.MANTA_CLIENT_ENCRYPTION_ENABLED_KEY,
                    config.isClientEncryptionEnabled());

            if (BooleanUtils.isTrue(config.isClientEncryptionEnabled())) {
                e.setContextValue(MapConfigContext.MANTA_ENCRYPTION_KEY_ID_KEY,
                        config.getEncryptionKeyId());
                e.setContextValue(MapConfigContext.MANTA_PERMIT_UNENCRYPTED_DOWNLOADS_KEY,
                        config.permitUnencryptedDownloads());
                e.setContextValue(MapConfigContext.MANTA_ENCRYPTION_ALGORITHM_KEY,
                        config.getEncryptionAlgorithm());
                e.setContextValue(MapConfigContext.MANTA_ENCRYPTION_PRIVATE_KEY_PATH_KEY,
                        config.getEncryptionPrivateKeyPath());
                e.setContextValue(MapConfigContext.MANTA_ENCRYPTION_PRIVATE_KEY_BYTES_KEY + "_size",
                        ArrayUtils.getLength(config.getEncryptionPrivateKeyBytes()));
            }

            throw e;
        }
    }

    /**
     * Utility method for validating that the client-side encryption
     * configuration has been instantiated with valid settings.
     *
     * @param config configuration to test
     * @param failureMessages list of messages to present the user for validation failures
     * @throws ConfigurationException thrown when validation fails
     */
    static void encryptionSettings(final ConfigContext config,
                                   final List<String> failureMessages) {
        // KEY ID VALIDATIONS

        if (config.getEncryptionKeyId() == null) {
            failureMessages.add("Encryption key id must not be null");
        }
        if (config.getEncryptionKeyId() != null && StringUtils.isEmpty(config.getEncryptionKeyId())) {
            failureMessages.add("Encryption key id must not be empty");
        }
        if (config.getEncryptionKeyId() != null && StringUtils.containsWhitespace(config.getEncryptionKeyId())) {
            failureMessages.add("Encryption key id must not contain whitespace");
        }
        if (config.getEncryptionKeyId() != null && !StringUtils.isAsciiPrintable(config.getEncryptionKeyId())) {
            failureMessages.add(("Encryption key id must only contain printable ASCII characters"));
        }

        // AUTHENTICATION MODE VALIDATIONS

        if (config.getEncryptionAuthenticationMode() == null) {
            failureMessages.add("Encryption authentication mode must not be null");
        }

        // PERMIT UNENCRYPTED DOWNLOADS VALIDATIONS

        if (config.permitUnencryptedDownloads() == null) {
            failureMessages.add("Permit unencrypted downloads flag must not be null");
        }

        // PRIVATE KEY VALIDATIONS

        if (config.getEncryptionPrivateKeyPath() == null && config.getEncryptionPrivateKeyBytes() == null) {
            failureMessages.add("Both encryption private key path and private key bytes must not be null");
        }

        if (config.getEncryptionPrivateKeyPath() != null && config.getEncryptionPrivateKeyBytes() != null) {
            failureMessages.add("Both encryption private key path and private key bytes must "
                    + "not be set. Choose one or the other.");
        }

        if (config.getEncryptionPrivateKeyPath() != null) {
            File keyFile = new File(config.getEncryptionPrivateKeyPath());

            if (!keyFile.exists()) {
                failureMessages.add(String.format("Key file couldn't be found at path: %s",
                        config.getEncryptionPrivateKeyPath()));
            } else if (!keyFile.canRead()) {
                failureMessages.add(String.format("Key file couldn't be read at path: %s",
                        config.getEncryptionPrivateKeyPath()));
            }
        }

        if (config.getEncryptionPrivateKeyBytes() != null) {
            if (config.getEncryptionPrivateKeyBytes().length == 0) {
                failureMessages.add("Encryption private key byte length must be greater than zero");
            }
        }

        // CIPHER ALGORITHM VALIDATIONS

        final String algorithm = config.getEncryptionAlgorithm();

        if (algorithm == null) {
            failureMessages.add("Encryption algorithm must not be null");
        }

        if (algorithm != null && StringUtils.isBlank(algorithm)) {
            failureMessages.add("Encryption algorithm must not be blank");
        }

        if (algorithm != null && !SupportedCiphersLookupMap.INSTANCE.containsKeyCaseInsensitive(algorithm)) {
            String availableAlgorithms = StringUtils.join(
                    SupportedCiphersLookupMap.INSTANCE.keySet(), ", ");
            failureMessages.add(String.format("Cipher algorithm [%s] was not found among "
                            + "the list of supported algorithms [%s]", algorithm,
                    availableAlgorithms));
        }
    }

    /**
     * Finds a configuration value based on a key name.
     *
     * @param attribute key name to search for
     * @param config configuration context to search within
     * @return null if not found, otherwise the configuration value
     */
    static Object getAttributeFromContext(final String attribute, final ConfigContext config) {
        switch (attribute) {
            case MapConfigContext.MANTA_URL_KEY:
            case EnvVarConfigContext.MANTA_URL_ENV_KEY:
                return config.getMantaURL();
            case MapConfigContext.MANTA_USER_KEY:
            case EnvVarConfigContext.MANTA_ACCOUNT_ENV_KEY:
                return config.getMantaUser();
            case MapConfigContext.MANTA_KEY_ID_KEY:
            case EnvVarConfigContext.MANTA_KEY_ID_ENV_KEY:
                return config.getMantaKeyId();
            case MapConfigContext.MANTA_KEY_PATH_KEY:
            case EnvVarConfigContext.MANTA_KEY_PATH_ENV_KEY:
                return config.getMantaKeyPath();
            case MapConfigContext.MANTA_TIMEOUT_KEY:
            case EnvVarConfigContext.MANTA_TIMEOUT_ENV_KEY:
                return config.getTimeout();
            case MapConfigContext.MANTA_RETRIES_KEY:
            case EnvVarConfigContext.MANTA_RETRIES_ENV_KEY:
                return config.getRetries();
            case MapConfigContext.MANTA_MAX_CONNS_KEY:
            case EnvVarConfigContext.MANTA_MAX_CONNS_ENV_KEY:
                return config.getMaximumConnections();
            case MapConfigContext.MANTA_PRIVATE_KEY_CONTENT_KEY:
            case EnvVarConfigContext.MANTA_PRIVATE_KEY_CONTENT_ENV_KEY:
                return config.getPrivateKeyContent();
            case MapConfigContext.MANTA_PASSWORD_KEY:
            case EnvVarConfigContext.MANTA_PASSWORD_ENV_KEY:
                return config.getPassword();
            case MapConfigContext.MANTA_HTTP_BUFFER_SIZE_KEY:
            case EnvVarConfigContext.MANTA_HTTP_BUFFER_SIZE_ENV_KEY:
                return config.getHttpBufferSize();
            case MapConfigContext.MANTA_HTTPS_PROTOCOLS_KEY:
            case EnvVarConfigContext.MANTA_HTTPS_PROTOCOLS_ENV_KEY:
                return config.getHttpsProtocols();
            case MapConfigContext.MANTA_HTTPS_CIPHERS_KEY:
            case EnvVarConfigContext.MANTA_HTTPS_CIPHERS_ENV_KEY:
                return config.getHttpsCipherSuites();
            case MapConfigContext.MANTA_NO_AUTH_KEY:
            case EnvVarConfigContext.MANTA_NO_AUTH_ENV_KEY:
                return config.noAuth();
            case MapConfigContext.MANTA_NO_NATIVE_SIGS_KEY:
            case EnvVarConfigContext.MANTA_NO_NATIVE_SIGS_ENV_KEY:
                return config.disableNativeSignatures();
            case MapConfigContext.MANTA_TCP_SOCKET_TIMEOUT_KEY:
            case EnvVarConfigContext.MANTA_TCP_SOCKET_TIMEOUT_ENV_KEY:
                return config.getTcpSocketTimeout();
            case MapConfigContext.MANTA_CLIENT_ENCRYPTION_ENABLED_KEY:
            case EnvVarConfigContext.MANTA_CLIENT_ENCRYPTION_ENABLED_ENV_KEY:
                return config.isClientEncryptionEnabled();
            case MapConfigContext.MANTA_VERIFY_UPLOADS_KEY:
            case EnvVarConfigContext.MANTA_VERIFY_UPLOADS_ENV_KEY:
                return config.verifyUploads();
            case MapConfigContext.MANTA_UPLOAD_BUFFER_SIZE_KEY:
            case EnvVarConfigContext.MANTA_UPLOAD_BUFFER_SIZE_ENV_KEY:
                return config.getUploadBufferSize();
            case MapConfigContext.MANTA_PERMIT_UNENCRYPTED_DOWNLOADS_KEY:
            case EnvVarConfigContext.MANTA_PERMIT_UNENCRYPTED_DOWNLOADS_ENV_KEY:
                return config.permitUnencryptedDownloads();
            case MapConfigContext.MANTA_ENCRYPTION_KEY_ID_KEY:
            case EnvVarConfigContext.MANTA_ENCRYPTION_KEY_ID_ENV_KEY:
                return config.getEncryptionKeyId();
            case MapConfigContext.MANTA_ENCRYPTION_ALGORITHM_KEY:
            case EnvVarConfigContext.MANTA_ENCRYPTION_ALGORITHM_ENV_KEY:
                return config.getEncryptionAlgorithm();
            case MapConfigContext.MANTA_ENCRYPTION_AUTHENTICATION_MODE_KEY:
            case EnvVarConfigContext.MANTA_ENCRYPTION_AUTHENTICATION_MODE_ENV_KEY:
                return config.getEncryptionAuthenticationMode();
            case MapConfigContext.MANTA_ENCRYPTION_PRIVATE_KEY_PATH_KEY:
            case EnvVarConfigContext.MANTA_ENCRYPTION_PRIVATE_KEY_PATH_ENV_KEY:
                return config.getEncryptionPrivateKeyPath();
            case MapConfigContext.MANTA_ENCRYPTION_PRIVATE_KEY_BYTES_KEY:
                return config.getEncryptionPrivateKeyBytes();
            case MapConfigContext.MANTA_ENCRYPTION_PRIVATE_KEY_BYTES_BASE64_KEY:
            case EnvVarConfigContext.MANTA_ENCRYPTION_PRIVATE_KEY_BYTES_BASE64_ENV_KEY:
                return Base64.getEncoder().encode(config.getEncryptionPrivateKeyBytes());
            default:
                return null;
        }
    }
}
