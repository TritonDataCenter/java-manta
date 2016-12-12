/*
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.config;

import java.util.Arrays;
import java.util.Objects;

/**
 * Abstract implementation of {@link ConfigContext} that allows for chaining
 * in default implementations of configuration that are delegate to when
 * we aren't passed a value.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
@SuppressWarnings("unused")
public abstract class BaseChainedConfigContext implements ConfigContext {
    /**
     * Manta service endpoint.
     */
    private String mantaURL;

    /**
     * Account associated with the Manta service.
     */
    private String account;

    /**
     * RSA key fingerprint of the private key used to access Manta.
     */
    private String mantaKeyId;

    /**
     * Path on the filesystem to the private RSA key used to access Manta.
     */
    private String mantaKeyPath;

    /**
     * General connection timeout for the Manta service.
     */
    private Integer timeout;

    /**
     * Number of times to retry failed requests.
     */
    private Integer retries;

    /**
     * The maximum number of open connections to the Manta API.
     */
    private Integer maxConnections;

    /**
     * Private key content. This shouldn't be set if the MantaKeyPath is set.
     */
    private String privateKeyContent;

    /**
     * Optional password for private key.
     */
    private String password;

    /**
     * This field has been deprecated.
     */
    private String httpTransport;

    /**
     * Comma delimited list of supported TLS protocols.
     */
    private String httpsProtocols;

    /**
     * Comma delimited list of supported TLS ciphers.
     */
    private String httpsCiphers;

    /**
     * Flag indicating if HTTP signatures are turned off.
     */
    private Boolean noAuth;

    /**
     * Flag indicating if HTTP signature native code generation is turned off.
     */
    private Boolean disableNativeSignatures;

    /**
     * Time in milliseconds to cache HTTP signature headers.
     */
    private Integer signatureCacheTTL;

    /**
     * Flag indicating when client-side encryption is enabled.
     */
    private Boolean clientEncryptionEnabled;

    /**
     * Flag indicating when downloading unencrypted files is allowed in
     * encryption mode.
     */
    private Boolean permitUnencryptedDownloads;

    /**
     * Enum specifying if we are in strict ciphertext authentication mode or not.
     */
    private EncryptionObjectAuthenticationMode encryptionAuthenticationMode;

    /**
     * Path to the private encryption key on the filesystem (can't be used if
     * private key bytes is not null).
     */
    private String encryptionPrivateKeyPath;

    /**
     * Private encryption key data (can't be used if private key path is not null).
     */
    private byte[] encryptionPrivateKeyBytes;

    /** Singleton instance of default configuration for easy reference. */
    public static final ConfigContext DEFAULT_CONFIG =
            new DefaultsConfigContext();

    /**
     * Constructor that pre-populates configuration context with the default
     * values.
     */
    public BaseChainedConfigContext() {
    }

    /**
     * Constructor that takes a default value for each one of the configuration
     * values.
     *
     * @param defaultingContext context that provides default values
     */
    public BaseChainedConfigContext(final ConfigContext defaultingContext) {
        overwriteWithContext(defaultingContext);
    }

    @Override
    public String getMantaURL() {
        return this.mantaURL;
    }

    @Override
    public String getMantaUser() {
        return this.account;
    }

    @Override
    public String getMantaKeyId() {
        return this.mantaKeyId;
    }

    @Override
    public String getMantaKeyPath() {
        return this.mantaKeyPath;
    }

    @Override
    public String getMantaHomeDirectory() {
        return ConfigContext.deriveHomeDirectoryFromUser(getMantaUser());
    }

    @Override
    public Integer getTimeout() {
        return this.timeout;
    }

    @Override
    public Integer getRetries() {
        return this.retries;
    }

    @Override
    public Integer getMaximumConnections() {
        return this.maxConnections;
    }

    @Override
    public String getPrivateKeyContent() {
        return this.privateKeyContent;
    }

    @Override
    public String getPassword() {
        return this.password;
    }

    @Override
    @Deprecated
    public String getHttpTransport() {
        return this.httpTransport;
    }

    @Override
    public String getHttpsProtocols() {
        return httpsProtocols;
    }

    @Override
    public String getHttpsCipherSuites() {
        return httpsCiphers;
    }

    @Override
    public Boolean noAuth() {
        return noAuth;
    }

    @Override
    public Boolean disableNativeSignatures() {
        return disableNativeSignatures;
    }

    @Override
    @Deprecated
    public Integer getSignatureCacheTTL() {
        return signatureCacheTTL;
    }

    @Override
    public Boolean isClientEncryptionEnabled() {
        return clientEncryptionEnabled;
    }

    @Override
    public Boolean permitUnencryptedDownloads() {
        return permitUnencryptedDownloads;
    }

    @Override
    public EncryptionObjectAuthenticationMode getEncryptionAuthenticationMode() {
        return encryptionAuthenticationMode;
    }

    /**
     * @return path to the private encryption key on the filesystem (can't be used if private key bytes is not null)
     */
    @Override
    public String getEncryptionPrivateKeyPath() {
        return encryptionPrivateKeyPath;
    }

    /**
     * @return private encryption key data (can't be used if private key path is not null)
     */
    @Override
    public byte[] getEncryptionPrivateKeyBytes() {
        return encryptionPrivateKeyBytes;
    }

    /**
     * Overwrites the configuration values with the values of the passed context
     * if those values are not null and aren't empty.
     *
     * @param context context to overwrite configuration with
     */
    public void overwriteWithContext(final ConfigContext context) {
        if (isPresent(context.getMantaURL())) {
            this.mantaURL = context.getMantaURL();
        }

        if (isPresent(context.getMantaUser())) {
            this.account = context.getMantaUser();
        }

        if (isPresent(context.getMantaKeyId())) {
            this.mantaKeyId = context.getMantaKeyId();
        }

        if (isPresent(context.getMantaKeyPath())) {
            if (isPresent(context.getPrivateKeyContent())) {
                String msg = "You can't set both a private key path and private key content";
                throw new IllegalArgumentException(msg);
            }
            this.mantaKeyPath = context.getMantaKeyPath();
        }

        if (context.getTimeout() != null) {
            this.timeout = context.getTimeout();
        }

        if (context.getRetries() != null) {
            this.retries = context.getRetries();
        }

        if (context.getMaximumConnections() != null) {
            this.maxConnections = context.getMaximumConnections();
        }

        if (isPresent(context.getPrivateKeyContent())) {
            if (isPresent(mantaKeyPath)) {
                String msg = "You can't set both a private key path and private key content";
                throw new IllegalArgumentException(msg);
            }

            this.privateKeyContent = context.getPrivateKeyContent();
        }

        if (isPresent(context.getPassword())) {
            this.password = context.getPassword();
        }

        if (isPresent(context.getHttpsProtocols())) {
            this.httpsProtocols = context.getHttpsProtocols();
        }

        if (isPresent(context.getHttpsCipherSuites())) {
            this.httpsCiphers = context.getHttpsCipherSuites();
        }

        if (context.noAuth() != null) {
            this.noAuth = context.noAuth();
        }

        if (context.disableNativeSignatures() != null) {
            this.disableNativeSignatures = context.disableNativeSignatures();
        }
    }

    /**
     * Checks to see that a given string is neither empty nor null.
     * @param string string to check
     * @return true when string is non-null and not empty
     */
    protected static boolean isPresent(final String string) {
        return string != null && !string.isEmpty();
    }

    /**
     * Sets the Manta service endpoint.
     * @param mantaURL Manta service endpoint
     * @return the current instance of {@link BaseChainedConfigContext}
     */
    public BaseChainedConfigContext setMantaURL(final String mantaURL) {
        this.mantaURL = mantaURL;
        return this;
    }

    /**
     * Sets the account associated with the Manta service.
     * @param mantaUser Manta user account
     * @return the current instance of {@link BaseChainedConfigContext}
     */
    public BaseChainedConfigContext setMantaUser(final String mantaUser) {
        this.account = mantaUser;
        return this;
    }

    /**
     * Sets the RSA key fingerprint of the private key used to access Manta.
     * @param mantaKeyId RSA key fingerprint
     * @return the current instance of {@link BaseChainedConfigContext}
     */
    public BaseChainedConfigContext setMantaKeyId(final String mantaKeyId) {
        this.mantaKeyId = mantaKeyId;
        return this;
    }

    /**
     * Sets the path on the filesystem to the private RSA key used to access Manta.
     * @param mantaKeyPath path on the filesystem
     * @return the current instance of {@link BaseChainedConfigContext}
     */
    public BaseChainedConfigContext setMantaKeyPath(final String mantaKeyPath) {
        if (isPresent(privateKeyContent)) {
            String msg = "You can't set both a private key path and private key content";
            throw new IllegalArgumentException(msg);
        }

        this.mantaKeyPath = mantaKeyPath;
        return this;
    }

    /**
     * Sets the general connection timeout for the Manta service.
     * @param timeout timeout in milliseconds
     * @return the current instance of {@link BaseChainedConfigContext}
     */
    public BaseChainedConfigContext setTimeout(final Integer timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * Sets the number of times to retry failed HTTP requests.
     * @param retries number of times to retry
     * @return the current instance of {@link BaseChainedConfigContext}
     */
    public BaseChainedConfigContext setRetries(final Integer retries) {
        if (retries < 0) {
            throw new IllegalArgumentException("Retries must be zero or greater");
        }
        this.retries = retries;
        return this;
    }

    /**
     * Sets the maximum number of open connections to the Manta API.
     * @param maxConns number of connections greater than zero
     * @return the current instance of {@link BaseChainedConfigContext}
     */
    public BaseChainedConfigContext setMaximumConnections(final Integer maxConns) {
        if (maxConns < 1) {
            throw new IllegalArgumentException("Maximum number of connections must "
                    + "be 1 or greater");
        }
        this.maxConnections = maxConns;

        return this;
    }

    /**
     * Sets the private key content used to authenticate. This can't be set if
     * you already have a private key path specified.
     * @param privateKeyContent contents of private key in plain text
     * @return the current instance of {@link BaseChainedConfigContext}
     */
    public BaseChainedConfigContext setPrivateKeyContent(final String privateKeyContent) {
        if (isPresent(mantaKeyPath)) {
            String msg = "You can't set both a private key path and private key content";
            throw new IllegalArgumentException(msg);
        }

        this.privateKeyContent = privateKeyContent;

        return this;
    }

    /**
     * Sets the password used for the private key. This is optional and not
     * typically used.
     * @param password password to set
     * @return the current instance of {@link BaseChainedConfigContext}
     */
    public BaseChainedConfigContext setPassword(final String password) {
        this.password = password;

        return this;
    }

    /**
     * This method is no longer used.
     *
     * @param httpTransport Typically 'ApacheHttpTransport' or 'NetHttpTransport'
     * @return the current instance of {@link BaseChainedConfigContext}
     */
    public BaseChainedConfigContext setHttpTransport(final String httpTransport) {
        this.httpTransport = httpTransport;

        return this;
    }

    /**
     * Set the supported TLS protocols.
     *
     * @param httpsProtocols comma delimited list of TLS protocols
     * @return the current instance of {@link BaseChainedConfigContext}
     */
    public BaseChainedConfigContext setHttpsProtocols(final String httpsProtocols) {
        this.httpsProtocols = httpsProtocols;

        return this;
    }

    /**
     * Set the supported TLS ciphers.
     *
     * @param httpsCiphers comma delimited list of TLS ciphers
     * @return the current instance of {@link BaseChainedConfigContext}
     */
    public BaseChainedConfigContext setHttpsCiphers(final String httpsCiphers) {
        this.httpsCiphers = httpsCiphers;

        return this;
    }

    /**
     * Change the state of whether or not HTTP signatures are sent to the Manta API.
     *
     * @param noAuth true to disable HTTP signatures
     * @return the current instance of {@link BaseChainedConfigContext}
     */
    public BaseChainedConfigContext setNoAuth(final Boolean noAuth) {
        this.noAuth = noAuth;

        return this;
    }

    /**
     * Change the state of whether or not HTTP signatures are using native code
     * to generate the cryptographic signatures.
     *
     * @param disableNativeSignatures true to disable
     * @return the current instance of {@link BaseChainedConfigContext}
     */
    public BaseChainedConfigContext setDisableNativeSignatures(final Boolean disableNativeSignatures) {
        this.disableNativeSignatures = disableNativeSignatures;

        return this;
    }

    /**
     * Sets the time in milliseconds to cache HTTP signature headers.
     *
     * @param signatureCacheTTL time in milliseconds to cache HTTP signature headers
     * @return the current instance of {@link BaseChainedConfigContext}
     */
    public BaseChainedConfigContext setSignatureCacheTTL(final Integer signatureCacheTTL) {
        this.signatureCacheTTL = signatureCacheTTL;

        return this;
    }

    /**
     * Sets the maximum number of open connections to the Manta API.
     *
     * @param maxConnections maximum number of open connections to open
     * @return the current instance of {@link BaseChainedConfigContext}
     */
    public BaseChainedConfigContext setMaxConnections(final Integer maxConnections) {
        this.maxConnections = maxConnections;

        return this;
    }

    /**
     * Sets flag indicating when client-side encryption is enabled.
     *
     * @param clientEncryptionEnabled true if client-side encryption is enabled
     * @return the current instance of {@link BaseChainedConfigContext}
     */
    public BaseChainedConfigContext setClientEncryptionEnabled(final Boolean clientEncryptionEnabled) {
        this.clientEncryptionEnabled = clientEncryptionEnabled;

        return this;
    }

    /**
     * Sets flag indicating when downloading unencrypted files is allowed in
     * encryption mode.
     *
     * @param permitUnencryptedDownloads true if downloading unencrypted data is permitted when in encrypted mode
     * @return the current instance of {@link BaseChainedConfigContext}
     */
    public BaseChainedConfigContext setPermitUnencryptedDownloads(final Boolean permitUnencryptedDownloads) {
        this.permitUnencryptedDownloads = permitUnencryptedDownloads;

        return this;
    }

    /**
     * Sets enum specifying if we are in strict ciphertext authentication mode
     * or not.
     *
     * @param encryptionAuthenticationMode enum of authentication mode
     * @return the current instance of {@link BaseChainedConfigContext}
     */
    public BaseChainedConfigContext setEncryptionAuthenticationMode(
            final EncryptionObjectAuthenticationMode encryptionAuthenticationMode) {
        this.encryptionAuthenticationMode = encryptionAuthenticationMode;

        return this;
    }

    /**
     * Sets the path to the private encryption key on the filesystem (can't be
     * used if private key bytes is not null).
     *
     * @param encryptionPrivateKeyPath path to private encryption key of file system
     * @return the current instance of {@link BaseChainedConfigContext}
     */
    public BaseChainedConfigContext setEncryptionPrivateKeyPath(final String encryptionPrivateKeyPath) {
        if (encryptionPrivateKeyBytes != null) {
            String msg = "You can't set both encryption key content and a private encryption key path";
            throw new IllegalArgumentException(msg);
        }
        this.encryptionPrivateKeyPath = encryptionPrivateKeyPath;

        return this;
    }

    /**
     * Sets the private encryption key data in memory (can't be used if private
     * key path is not null).
     *
     * @param encryptionPrivateKeyBytes byte array containing private key data
     * @return the current instance of {@link BaseChainedConfigContext}
     */
    public BaseChainedConfigContext setEncryptionPrivateKeyBytes(final byte[] encryptionPrivateKeyBytes) {
        if (isPresent(encryptionPrivateKeyPath)) {
            String msg = "You can't set both a private encryption key path and encryption key content";
            throw new IllegalArgumentException(msg);
        }

        this.encryptionPrivateKeyBytes = encryptionPrivateKeyBytes;

        return this;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        BaseChainedConfigContext that = (BaseChainedConfigContext) other;
        return Objects.equals(mantaURL, that.mantaURL)
                && Objects.equals(account, that.account)
                && Objects.equals(mantaKeyId, that.mantaKeyId)
                && Objects.equals(mantaKeyPath, that.mantaKeyPath)
                && Objects.equals(timeout, that.timeout)
                && Objects.equals(retries, that.retries)
                && Objects.equals(maxConnections, that.maxConnections)
                && Objects.equals(privateKeyContent, that.privateKeyContent)
                && Objects.equals(password, that.password)
                && Objects.equals(httpTransport, that.httpTransport)
                && Objects.equals(httpsProtocols, that.httpsProtocols)
                && Objects.equals(httpsCiphers, that.httpsCiphers)
                && Objects.equals(noAuth, that.noAuth)
                && Objects.equals(disableNativeSignatures, that.disableNativeSignatures)
                && Objects.equals(signatureCacheTTL, that.signatureCacheTTL)
                && Objects.equals(clientEncryptionEnabled, that.clientEncryptionEnabled)
                && Objects.equals(permitUnencryptedDownloads, that.permitUnencryptedDownloads)
                && Objects.equals(encryptionAuthenticationMode, that.encryptionAuthenticationMode)
                && Objects.equals(encryptionPrivateKeyPath, that.encryptionPrivateKeyPath)
                && Arrays.equals(encryptionPrivateKeyBytes, that.encryptionPrivateKeyBytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mantaURL, account, mantaKeyId, mantaKeyPath,
                timeout, retries, maxConnections, privateKeyContent, password,
                httpTransport, httpsProtocols, httpsCiphers, noAuth,
                disableNativeSignatures, signatureCacheTTL,
                clientEncryptionEnabled, permitUnencryptedDownloads,
                encryptionAuthenticationMode, encryptionPrivateKeyPath,
                encryptionPrivateKeyBytes);
    }

    @Override
    public String toString() {
        return ConfigContext.toString(this);
    }
}
