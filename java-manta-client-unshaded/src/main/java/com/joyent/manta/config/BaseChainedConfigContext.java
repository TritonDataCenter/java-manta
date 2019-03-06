/*
 * Copyright (c) 2019, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
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
 * @author <a href="https://github.com/nairashwin952013">Ashwin A Nair</a>
 */
@SuppressWarnings("unused")
public abstract class BaseChainedConfigContext implements SettableConfigContext<BaseChainedConfigContext> {
    /**
     * Manta service endpoint.
     */
    private volatile String mantaURL;

    /**
     * Account associated with the Manta service.
     */
    private volatile String account;

    /**
     * RSA key fingerprint of the private key used to access Manta.
     */
    private volatile String mantaKeyId;

    /**
     * Path on the filesystem to the private RSA key used to access Manta.
     */
    private volatile String mantaKeyPath;

    /**
     * Private key content. This shouldn't be set if the MantaKeyPath is set.
     */
    private volatile String privateKeyContent;

    /**
     * Optional password for private key.
     */
    private volatile String password;

    /**
     * Flag indicating if HTTP signatures are turned off.
     */
    private volatile Boolean noAuth;

    /**
     * Flag indicating if HTTP signature native code generation is turned off.
     */
    private volatile Boolean disableNativeSignatures;

    /**
     * Flag indicating if we verify the uploaded file's checksum against the
     * server's checksum (MD5).
     */
    private volatile Boolean verifyUploads;

    /**
     * General connection timeout for the Manta service.
     */
    private volatile Integer timeout;

    /**
     * Number of times to retry failed requests.
     */
    private volatile Integer retries;

    /**
     * The maximum number of open connections to the Manta API.
     */
    private volatile Integer maxConnections;

    /**
     * Size of buffer in bytes to use to buffer streams of HTTP data.
     */
    private volatile Integer httpBufferSize;

    /**
     * Comma delimited list of supported TLS protocols.
     */
    private volatile String httpsProtocols;

    /**
     * Comma delimited list of supported TLS ciphers.
     */
    private volatile String httpsCipherSuites;

    /**
     * Time in milliseconds to cache HTTP signature headers.
     */
    private volatile Integer tcpSocketTimeout;

    /**
     * Time in milliseconds to wait for a connection from the connection pool.
     */
    private volatile Integer connectionRequestTimeout;

    /**
     * When not null, time in milliseconds to wait for a 100-continue response.
     */
    private volatile Integer expectContinueTimeout;

    /**
     * Number of bytes to read into memory for a streaming upload before
     * deciding if we want to load it in memory before send it.
     */
    private volatile Integer uploadBufferSize;

    /**
     * Number of directories to assume exist when recursively creating directories.
     */
    private volatile Integer skipDirectoryDepth;

    /**
     * Clean up as many parent directories as possible after deleting an object.
     * This is the greatest number of directories that will be attempted
     * to be deleted.
     */
    private volatile Integer pruneEmptyParentDepth;

    /**
     * Whether or not we can attempt to resume a download automatically.
     */
    private volatile Integer downloadContinuations;

    /**
     * Whether metrics and MBeans should be tracked and exposed.
     */
    private volatile MetricReporterMode metricReporterMode;

    /**
     * Metrics output interval in seconds for modes that report metrics periodically.
     */
    private volatile Integer metricReporterOutputInterval;

    /**
     * Flag indicating when client-side encryption is enabled.
     */
    private volatile Boolean clientEncryptionEnabled;

    /**
     * Flag indicating if automatic content type detection is enabled while uploading files in Manta.
     */
    private volatile Boolean contentTypeDetectionEnabled;

    /**
     * The unique identifier of the key used for encryption.
     */
    private volatile String encryptionKeyId;

    /**
     * The client encryption algorithm name in the format of
     * <code>cipher/mode/padding state</code>.
     */
    private volatile String encryptionAlgorithm;

    /**
     * Flag indicating when downloading unencrypted files is allowed in
     * encryption mode.
     */
    private volatile Boolean permitUnencryptedDownloads;

    /**
     * Enum specifying if we are in strict ciphertext authentication mode or not.
     */
    private volatile EncryptionAuthenticationMode encryptionAuthenticationMode;

    /**
     * Path to the private encryption key on the filesystem (can't be used if
     * private key bytes is not null).
     */
    private volatile String encryptionPrivateKeyPath;

    /**
     * Private encryption key data (can't be used if private key path is not null).
     */
    private volatile byte[] encryptionPrivateKeyBytes;

    /**
     * Flag indicating the the mantaKeyPath has been set only by the defaults.
     * True = set by defaults.
     * False = overwritten by non-defaults.
     */
    private volatile boolean mantaKeyPathSetOnlyByDefaults = false;

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
    public Integer getHttpBufferSize() {
        return this.httpBufferSize;
    }

    @Override
    public String getHttpsProtocols() {
        return httpsProtocols;
    }

    @Override
    public String getHttpsCipherSuites() {
        return httpsCipherSuites;
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
    public Integer getTcpSocketTimeout() {
        return tcpSocketTimeout;
    }

    @Override
    public Integer getConnectionRequestTimeout() {
        return connectionRequestTimeout;
    }

    @Override
    public Integer getExpectContinueTimeout() {
        return expectContinueTimeout;
    }

    @Override
    public Boolean verifyUploads() {
        return verifyUploads;
    }

    @Override
    public Integer getSkipDirectoryDepth() {
        return this.skipDirectoryDepth;
    }

    @Override
    public Integer getPruneEmptyParentDepth() {
        return this.pruneEmptyParentDepth;
    }

    @Override
    public Integer downloadContinuations() {
        return this.downloadContinuations;
    }

    @Override
    public MetricReporterMode getMetricReporterMode() {
        return this.metricReporterMode;
    }

    @Override
    public Integer getMetricReporterOutputInterval() {
        return this.metricReporterOutputInterval;
    }

    @Override
    public Integer getUploadBufferSize() {
        return uploadBufferSize;
    }

    @Override
    public Boolean isClientEncryptionEnabled() {
        return clientEncryptionEnabled;
    }

    @Override
    public Boolean isContentTypeDetectionEnabled() {
        return contentTypeDetectionEnabled;
    }

    @Override
    public String getEncryptionKeyId() {
        return encryptionKeyId;
    }

    @Override
    public String getEncryptionAlgorithm() {
        return encryptionAlgorithm;
    }

    @Override
    public Boolean permitUnencryptedDownloads() {
        return permitUnencryptedDownloads;
    }

    @Override
    public EncryptionAuthenticationMode getEncryptionAuthenticationMode() {
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
    @SuppressWarnings("Duplicates")
    public void overwriteWithContext(final ConfigContext context) {
        /* If a default context is being used to overwrite after this
         * context has been initialized, then we want to be careful to not
         * overwrite values that have already been set by non-default contexts. */
        boolean isDefaultContext = context.getClass().equals(DefaultsConfigContext.class);

        if (isDefaultContext) {
            overwriteWithDefaultContext((DefaultsConfigContext)context);
            return;
        }

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

        if (context.getMetricReporterMode() != null) {
            this.metricReporterMode = context.getMetricReporterMode();
        }

        if (context.getMetricReporterOutputInterval() != null) {
            this.metricReporterOutputInterval = context.getMetricReporterOutputInterval();
        }

        if (context.getRetries() != null) {
            this.retries = context.getRetries();
        }

        if (context.getMaximumConnections() != null) {
            this.maxConnections = context.getMaximumConnections();
        }

        if (isPresent(context.getPrivateKeyContent())) {
            if (isPresent(mantaKeyPath) && !this.mantaKeyPathSetOnlyByDefaults) {
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
            this.httpsCipherSuites = context.getHttpsCipherSuites();
        }

        if (context.noAuth() != null) {
            this.noAuth = context.noAuth();
        }

        if (context.disableNativeSignatures() != null) {
            this.disableNativeSignatures = context.disableNativeSignatures();
        }

        if (context.getHttpBufferSize() != null) {
            this.httpBufferSize = context.getHttpBufferSize();
        }

        overwriteWithContextTimeouts(context);

        if (context.verifyUploads() != null) {
            this.verifyUploads = context.verifyUploads();
        }

        if (context.getUploadBufferSize() != null) {
            this.uploadBufferSize = context.getUploadBufferSize();
        }

        if (context.getSkipDirectoryDepth() != null) {
            this.skipDirectoryDepth = context.getSkipDirectoryDepth();
        }

        if (context.getPruneEmptyParentDepth() != null) {
            this.pruneEmptyParentDepth = context.getPruneEmptyParentDepth();
        }

        if (context.downloadContinuations() != null) {
            this.downloadContinuations = context.downloadContinuations();
        }

        if (context.getMetricReporterMode() != null) {
            this.metricReporterMode = context.getMetricReporterMode();
        }

        if (context.getMetricReporterOutputInterval() != null) {
            this.metricReporterOutputInterval = context.getMetricReporterOutputInterval();
        }

        if (context.isContentTypeDetectionEnabled() != null) {
            this.contentTypeDetectionEnabled = context.isContentTypeDetectionEnabled();
        }

        overwriteWithContextEncryptionParams(context);

    }

    /**
     * Overwrites the configuration timeouts values with values of passed context
     * if those values are not null and aren't empty.
     *
     * @param context context to overwrite configuration with
     */
    private void overwriteWithContextTimeouts(final ConfigContext context) {
        boolean isDefaultContext = context.getClass().equals(DefaultsConfigContext.class);

        if (context.getTimeout() != null) {
            this.timeout = context.getTimeout();
        }

        if (context.getTcpSocketTimeout() != null) {
            this.tcpSocketTimeout = context.getTcpSocketTimeout();
        }

        if (context.getConnectionRequestTimeout() != null) {
            this.connectionRequestTimeout = context.getConnectionRequestTimeout();
        }

        if (context.getExpectContinueTimeout() != null) {
            this.expectContinueTimeout = context.getExpectContinueTimeout();
        }

    }

    /**
     * Overwrites the configuration values with values of passed context
     * if those values are not null and aren't empty.
     *
     * @param context context to overwrite configuration with
     */
    private void overwriteWithContextEncryptionParams(final ConfigContext context) {
        boolean isDefaultContext = context.getClass().equals(DefaultsConfigContext.class);

        if (context.isClientEncryptionEnabled() != null) {
            this.clientEncryptionEnabled = context.isClientEncryptionEnabled();
        }

        if (context.getEncryptionKeyId() != null) {
            this.encryptionKeyId = context.getEncryptionKeyId();
        }

        if (context.getEncryptionAlgorithm() != null) {
            this.encryptionAlgorithm = context.getEncryptionAlgorithm();
        }

        if (context.getEncryptionAuthenticationMode() != null) {
            this.encryptionAuthenticationMode = context.getEncryptionAuthenticationMode();
        }

        if (context.getEncryptionPrivateKeyPath() != null) {
            this.encryptionPrivateKeyPath = context.getEncryptionPrivateKeyPath();
        }

        if (context.getEncryptionPrivateKeyBytes() != null) {
            this.encryptionPrivateKeyBytes = context.getEncryptionPrivateKeyBytes();
        }

        if (context.permitUnencryptedDownloads() != null) {
            this.permitUnencryptedDownloads = context.permitUnencryptedDownloads();
        }

    }

    /**
     * Overwrites this context with the supplied defaults context instance.
     *
     * @param context default configuration context
     */
    protected void overwriteWithDefaultContext(final DefaultsConfigContext context) {
        if (!isPresent(this.getMantaURL())) {
            this.mantaURL = context.getMantaURL();
        }

        if (!isPresent(this.getMantaUser())) {
            this.account = context.getMantaUser();
        }

        if (!isPresent(this.getMantaKeyId())) {
            this.mantaKeyId = context.getMantaKeyId();
        }

        if (!isPresent(this.getMantaKeyPath()) && !isPresent(this.getPrivateKeyContent())) {
            this.mantaKeyPathSetOnlyByDefaults = true;
            this.mantaKeyPath = context.getMantaKeyPath();
        }

        if (this.getTimeout() == null) {
            this.timeout = context.getTimeout();
        }

        if (this.getRetries() == null) {
            this.retries = context.getRetries();
        }

        if (this.getMaximumConnections() == null) {
            this.maxConnections = context.getMaximumConnections();
        }

        if (!isPresent(this.getPassword())) {
            this.password = context.getPassword();
        }

        if (!isPresent(this.getHttpsProtocols())) {
            this.httpsProtocols = context.getHttpsProtocols();
        }

        if (!isPresent(this.getHttpsCipherSuites())) {
            this.httpsCipherSuites = context.getHttpsCipherSuites();
        }

        if (this.noAuth() == null) {
            this.noAuth = context.noAuth();
        }

        if (this.disableNativeSignatures() == null) {
            this.disableNativeSignatures = context.disableNativeSignatures();
        }

        if (this.httpBufferSize == null) {
            this.httpBufferSize = context.getHttpBufferSize();
        }

        if (this.tcpSocketTimeout == null) {
            this.tcpSocketTimeout = context.getTcpSocketTimeout();
        }

        if (this.connectionRequestTimeout == null) {
            this.connectionRequestTimeout = context.getConnectionRequestTimeout();
        }

        if (this.expectContinueTimeout == null) {
            this.expectContinueTimeout = context.getExpectContinueTimeout();
        }

        if (this.verifyUploads == null) {
            this.verifyUploads = context.verifyUploads();
        }

        if (this.uploadBufferSize == null) {
            this.uploadBufferSize = context.getUploadBufferSize();
        }

        if (this.skipDirectoryDepth == null) {
            this.skipDirectoryDepth = context.getSkipDirectoryDepth();
        }

        if (this.pruneEmptyParentDepth == null) {
            this.pruneEmptyParentDepth = context.getPruneEmptyParentDepth();
        }

        if (this.downloadContinuations == null) {
            this.downloadContinuations = context.downloadContinuations();
        }

        if (this.getMetricReporterMode() == null) {
            this.metricReporterMode = context.getMetricReporterMode();
        }

        if (this.getMetricReporterOutputInterval() == null) {
            this.metricReporterOutputInterval = context.getMetricReporterOutputInterval();
        }

        if (this.clientEncryptionEnabled == null) {
            this.clientEncryptionEnabled = context.isClientEncryptionEnabled();
        }

        if (this.contentTypeDetectionEnabled == null) {
            this.contentTypeDetectionEnabled = context.isContentTypeDetectionEnabled();
        }

        if (this.encryptionKeyId == null) {
            this.encryptionKeyId = context.getEncryptionKeyId();
        }

        if (this.encryptionAlgorithm == null) {
            this.encryptionAlgorithm = context.getEncryptionAlgorithm();
        }

        if (this.encryptionAuthenticationMode == null) {
            this.encryptionAuthenticationMode = context.getEncryptionAuthenticationMode();
        }

        if (this.encryptionPrivateKeyPath == null) {
            this.encryptionPrivateKeyPath = context.getEncryptionPrivateKeyPath();
        }

        if (this.permitUnencryptedDownloads == null) {
            this.permitUnencryptedDownloads = context.permitUnencryptedDownloads();
        }

        /* Note: we purposely omitted privateKeyContent and
         * EncryptionPrivateKeyBytes because there are never going to
         * be defaults for those values.
         */
    }

    /**
     * Checks to see that a given string is neither empty nor null.
     * @param string string to check
     * @return true when string is non-null and not empty
     */
    protected static boolean isPresent(final String string) {
        return string != null && !string.isEmpty();
    }

    @Override
    public BaseChainedConfigContext setMantaURL(final String mantaURL) {
        this.mantaURL = mantaURL;
        return this;
    }

    @Override
    public BaseChainedConfigContext setMantaUser(final String mantaUser) {
        this.account = mantaUser;
        return this;
    }

    @Override
    public BaseChainedConfigContext setMantaKeyId(final String mantaKeyId) {
        this.mantaKeyId = mantaKeyId;
        return this;
    }

    @Override
    public BaseChainedConfigContext setMantaKeyPath(final String mantaKeyPath) {
        if (isPresent(privateKeyContent)) {
            String msg = "You can't set both a private key path and private key content";
            throw new IllegalArgumentException(msg);
        }

        this.mantaKeyPath = mantaKeyPath;
        return this;
    }

    @Override
    public BaseChainedConfigContext setTimeout(final Integer timeout) {
        this.timeout = timeout;
        return this;
    }

    @Override
    public BaseChainedConfigContext setRetries(final Integer retries) {
        if (retries < 0) {
            throw new IllegalArgumentException("Retries must be zero or greater");
        }
        this.retries = retries;
        return this;
    }

    @Override
    public BaseChainedConfigContext setMaximumConnections(final Integer maxConns) {
        if (maxConns < 1) {
            throw new IllegalArgumentException("Maximum number of connections must "
                    + "be 1 or greater");
        }
        this.maxConnections = maxConns;

        return this;
    }

    @Override
    public BaseChainedConfigContext setPrivateKeyContent(final String privateKeyContent) {
        if (isPresent(mantaKeyPath)) {
            String msg = "You can't set both a private key path and private key content";
            throw new IllegalArgumentException(msg);
        }

        this.privateKeyContent = privateKeyContent;

        return this;
    }

    @Override
    public BaseChainedConfigContext setPassword(final String password) {
        this.password = password;

        return this;
    }

    @Override
    public BaseChainedConfigContext setHttpBufferSize(final Integer httpBufferSize) {
        this.httpBufferSize = httpBufferSize;

        return this;
    }

    @Override
    public BaseChainedConfigContext setHttpsProtocols(final String httpsProtocols) {
        this.httpsProtocols = httpsProtocols;

        return this;
    }

    @Override
    public BaseChainedConfigContext setHttpsCipherSuites(final String httpsCipherSuites) {
        this.httpsCipherSuites = httpsCipherSuites;

        return this;
    }

    @Override
    public BaseChainedConfigContext setNoAuth(final Boolean noAuth) {
        this.noAuth = noAuth;

        return this;
    }

    @Override
    public BaseChainedConfigContext setDisableNativeSignatures(final Boolean disableNativeSignatures) {
        this.disableNativeSignatures = disableNativeSignatures;

        return this;
    }

    @Override
    public BaseChainedConfigContext setTcpSocketTimeout(final Integer tcpSocketTimeout) {
        this.tcpSocketTimeout = tcpSocketTimeout;

        return this;
    }

    @Override
    public BaseChainedConfigContext setConnectionRequestTimeout(final Integer connectionRequestTimeout) {
        this.connectionRequestTimeout = connectionRequestTimeout;

        return this;
    }

    @Override
    public BaseChainedConfigContext setExpectContinueTimeout(final Integer expectContinueTimeout) {
        this.expectContinueTimeout = expectContinueTimeout;

        return this;
    }

    @Override
    public BaseChainedConfigContext setVerifyUploads(final Boolean verify) {
        this.verifyUploads = verify;

        return this;
    }

    @Override
    public BaseChainedConfigContext setUploadBufferSize(final Integer size) {
        this.uploadBufferSize = size;

        return this;
    }

    @Override
    public BaseChainedConfigContext setSkipDirectoryDepth(final Integer depth) {
        this.skipDirectoryDepth = depth;

        return this;
    }

    @Override
    public BaseChainedConfigContext setPruneEmptyParentDepth(final Integer depth) {
        this.pruneEmptyParentDepth = depth;

        return this;
    }

    @Override
    public BaseChainedConfigContext setDownloadContinuations(final Integer continuation) {
        this.downloadContinuations = continuation;

        return this;
    }

    @Override
    public BaseChainedConfigContext setMetricReporterMode(final MetricReporterMode metricReporterMode) {
        this.metricReporterMode = metricReporterMode;
        return this;
    }

    @Override
    public BaseChainedConfigContext setMetricReporterOutputInterval(final Integer metricReporterOutputInterval) {
        this.metricReporterOutputInterval = metricReporterOutputInterval;
        return this;
    }

    @Override
    public BaseChainedConfigContext setClientEncryptionEnabled(final Boolean clientEncryptionEnabled) {
        this.clientEncryptionEnabled = clientEncryptionEnabled;

        return this;
    }

    @Override
    public BaseChainedConfigContext setContentTypeDetectionEnabled(final Boolean contentTypeDetectionEnabled) {
        this.contentTypeDetectionEnabled = contentTypeDetectionEnabled;

        return this;
    }

    @Override
    public BaseChainedConfigContext setEncryptionKeyId(final String keyId) {
        this.encryptionKeyId = keyId;

        return this;
    }

    @Override
    public BaseChainedConfigContext setEncryptionAlgorithm(final String algorithm) {
        this.encryptionAlgorithm = algorithm;

        return this;
    }

    @Override
    public BaseChainedConfigContext setPermitUnencryptedDownloads(final Boolean permitUnencryptedDownloads) {
        this.permitUnencryptedDownloads = permitUnencryptedDownloads;

        return this;
    }

    @Override
    public BaseChainedConfigContext setEncryptionAuthenticationMode(
            final EncryptionAuthenticationMode encryptionAuthenticationMode) {
        this.encryptionAuthenticationMode = encryptionAuthenticationMode;

        return this;
    }

    @Override
    public BaseChainedConfigContext setEncryptionPrivateKeyPath(final String encryptionPrivateKeyPath) {
        if (encryptionPrivateKeyBytes != null) {
            String msg = "You can't set both encryption key content and a private encryption key path";
            throw new IllegalArgumentException(msg);
        }
        this.encryptionPrivateKeyPath = encryptionPrivateKeyPath;

        return this;
    }

    @Override
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

        if (!(other instanceof BaseChainedConfigContext)) {
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
                && Objects.equals(httpBufferSize, that.httpBufferSize)
                && Objects.equals(httpsProtocols, that.httpsProtocols)
                && Objects.equals(httpsCipherSuites, that.httpsCipherSuites)
                && Objects.equals(noAuth, that.noAuth)
                && Objects.equals(disableNativeSignatures, that.disableNativeSignatures)
                && Objects.equals(tcpSocketTimeout, that.tcpSocketTimeout)
                && Objects.equals(connectionRequestTimeout, that.connectionRequestTimeout)
                && Objects.equals(expectContinueTimeout, that.expectContinueTimeout)
                && Objects.equals(verifyUploads, that.verifyUploads)
                && Objects.equals(uploadBufferSize, that.uploadBufferSize)
                && Objects.equals(skipDirectoryDepth, that.skipDirectoryDepth)
                && Objects.equals(pruneEmptyParentDepth, that.pruneEmptyParentDepth)
                && Objects.equals(downloadContinuations, that.downloadContinuations)
                && Objects.equals(metricReporterMode, that.metricReporterMode)
                && Objects.equals(metricReporterOutputInterval, that.metricReporterOutputInterval)
                && Objects.equals(clientEncryptionEnabled, that.clientEncryptionEnabled)
                && Objects.equals(contentTypeDetectionEnabled, that.contentTypeDetectionEnabled)
                && Objects.equals(encryptionKeyId, that.encryptionKeyId)
                && Objects.equals(encryptionAlgorithm, that.encryptionAlgorithm)
                && Objects.equals(permitUnencryptedDownloads, that.permitUnencryptedDownloads)
                && Objects.equals(encryptionAuthenticationMode, that.encryptionAuthenticationMode)
                && Objects.equals(encryptionPrivateKeyPath, that.encryptionPrivateKeyPath)
                && Arrays.equals(encryptionPrivateKeyBytes, that.encryptionPrivateKeyBytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mantaURL, account, mantaKeyId, mantaKeyPath,
                timeout, retries, maxConnections, privateKeyContent, password,
                httpBufferSize, httpsProtocols, httpsCipherSuites, noAuth,
                disableNativeSignatures,
                tcpSocketTimeout, connectionRequestTimeout, expectContinueTimeout,
                verifyUploads, uploadBufferSize,
                skipDirectoryDepth,
                downloadContinuations,
                metricReporterMode,
                metricReporterOutputInterval,
                clientEncryptionEnabled, encryptionKeyId,
                contentTypeDetectionEnabled,
                encryptionAlgorithm, permitUnencryptedDownloads,
                encryptionAuthenticationMode, encryptionPrivateKeyPath,
                Arrays.hashCode(encryptionPrivateKeyBytes));
    }

    @Override
    public String toString() {
        return ConfigContext.toString(this);
    }
}
