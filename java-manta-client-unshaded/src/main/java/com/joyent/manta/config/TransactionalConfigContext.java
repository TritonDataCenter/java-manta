/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.config;

import java.util.concurrent.locks.ReentrantReadWriteLock;

@SuppressWarnings({"checkstyle:JavadocVariable", "checkstyle:JavadocType", "checkstyle:JavadocMethod"})
public class TransactionalConfigContext extends StandardConfigContext implements Reloadable<TransactionalConfigContext> {

    private final ReentrantReadWriteLock.ReadLock readLock;

    private final ReentrantReadWriteLock.WriteLock writeLock;

    private volatile AuthAwareConfigContext.AuthContext authContext;

    /**
     * Build an empty AuthAwareConfigContext.
     */
    public TransactionalConfigContext() {
        this(null);
    }

    /**
     * Build an AuthAwareConfigContext from an existing {@link ConfigContext}.
     *
     * @param config configuration context from which to extract values
     */
    public TransactionalConfigContext(final ConfigContext config) {
        final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
        readLock = rwLock.readLock();
        writeLock = rwLock.writeLock();

        if (config != null) {
            overwriteWithContext(config);
        }

        reload();
    }

    /* ======================================================================
     * TRANSACTIONS
     * ====================================================================== */

    public void lock() {
        if (writeLock.isHeldByCurrentThread()) {
            throw new IllegalStateException("Unexpected lock, already held by current thread.");
        }

        writeLock.lock();
    }

    public void unlock() {
        if (!writeLock.isHeldByCurrentThread()) {
            throw new IllegalStateException("Unexpected unlock, thread does not hold lock");
        }

        writeLock.unlock();
    }

    public TransactionalConfigContext reload() {
        lock();
        final int newParamsFingerprint = ConfigContext.calculateAuthParamsFingerprint(this);

        if (authContext != null) {
            if (authContext.paramsFingerprint == newParamsFingerprint) {
                unlock();
                return this;
            } else {
                authContext.signer.clearAll();
                authContext = null;
            }
        }

        authContext = ConfigContext.loadAuthContext(newParamsFingerprint, this);
        unlock();

        return this;
    }

    /* ======================================================================
     * GETTERS
     * ====================================================================== */

    @Override
    public String getMantaURL() {
        readLock.lock();
        final String value = super.getMantaURL();
        readLock.unlock();
        return value;
    }

    @Override
    public String getMantaUser() {
        readLock.lock();
        final String value = super.getMantaUser();
        readLock.unlock();
        return value;
    }

    @Override
    public String getMantaKeyId() {
        readLock.lock();
        final String value = super.getMantaKeyId();
        readLock.unlock();
        return value;
    }

    @Override
    public String getMantaKeyPath() {
        readLock.lock();
        final String value = super.getMantaKeyPath();
        readLock.unlock();
        return value;
    }

    @Override
    public Integer getTimeout() {
        readLock.lock();
        final Integer value = super.getTimeout();
        readLock.unlock();
        return value;
    }

    @Override
    public Integer getRetries() {
        readLock.lock();
        final Integer value = super.getRetries();
        readLock.unlock();
        return value;
    }

    @Override
    public Integer getMaximumConnections() {
        readLock.lock();
        final Integer value = super.getMaximumConnections();
        readLock.unlock();
        return value;
    }

    @Override
    public String getPrivateKeyContent() {
        readLock.lock();
        final String value = super.getPrivateKeyContent();
        readLock.unlock();
        return value;
    }

    @Override
    public String getPassword() {
        readLock.lock();
        final String value = super.getPassword();
        readLock.unlock();
        return value;
    }

    @Override
    public Integer getHttpBufferSize() {
        readLock.lock();
        final Integer value = super.getHttpBufferSize();
        readLock.unlock();
        return value;
    }

    @Override
    public String getHttpsProtocols() {
        readLock.lock();
        final String value = super.getHttpsProtocols();
        readLock.unlock();
        return value;
    }

    @Override
    public String getHttpsCipherSuites() {
        readLock.lock();
        final String value = super.getHttpsCipherSuites();
        readLock.unlock();
        return value;
    }

    @Override
    public Boolean noAuth() {
        readLock.lock();
        final Boolean value = super.noAuth();
        readLock.unlock();
        return value;
    }

    @Override
    public Boolean disableNativeSignatures() {
        readLock.lock();
        final Boolean value = super.disableNativeSignatures();
        readLock.unlock();
        return value;
    }

    @Override
    public Integer getTcpSocketTimeout() {
        readLock.lock();
        final Integer value = super.getTcpSocketTimeout();
        readLock.unlock();
        return value;
    }

    @Override
    public Integer getConnectionRequestTimeout() {
        readLock.lock();
        final Integer value = super.getConnectionRequestTimeout();
        readLock.unlock();
        return value;
    }

    @Override
    public Boolean verifyUploads() {
        readLock.lock();
        final Boolean value = super.verifyUploads();
        readLock.unlock();
        return value;
    }

    @Override
    public Integer getUploadBufferSize() {
        readLock.lock();
        final Integer value = super.getUploadBufferSize();
        readLock.unlock();
        return value;
    }

    @Override
    public Boolean isClientEncryptionEnabled() {
        readLock.lock();
        final Boolean value = super.isClientEncryptionEnabled();
        readLock.unlock();
        return value;
    }

    @Override
    public String getEncryptionKeyId() {
        readLock.lock();
        final String value = super.getEncryptionKeyId();
        readLock.unlock();
        return value;
    }

    @Override
    public String getEncryptionAlgorithm() {
        readLock.lock();
        final String value = super.getEncryptionAlgorithm();
        readLock.unlock();
        return value;
    }

    @Override
    public Boolean permitUnencryptedDownloads() {
        readLock.lock();
        final Boolean value = super.permitUnencryptedDownloads();
        readLock.unlock();
        return value;
    }

    @Override
    public EncryptionAuthenticationMode getEncryptionAuthenticationMode() {
        readLock.lock();
        final EncryptionAuthenticationMode value = super.getEncryptionAuthenticationMode();
        readLock.unlock();
        return value;
    }

    @Override
    public String getEncryptionPrivateKeyPath() {
        readLock.lock();
        final String value = super.getEncryptionPrivateKeyPath();
        readLock.unlock();
        return value;
    }

    @Override
    public byte[] getEncryptionPrivateKeyBytes() {
        readLock.lock();
        final byte[] value = super.getEncryptionPrivateKeyBytes();
        readLock.unlock();
        return value;
    }

    /* ======================================================================
     * SETTERS
     * ====================================================================== */

    @Override
    public TransactionalConfigContext setMantaURL(final String mantaURL) {
        final boolean didLock = grabWriteLockIfNotHeldByCurrentThread();
        super.setMantaURL(mantaURL);
        if (didLock) {
            writeLock.unlock();
        }
        return this;
    }

    @Override
    public TransactionalConfigContext setMantaUser(final String mantaUser) {
        final boolean didLock = grabWriteLockIfNotHeldByCurrentThread();
        super.setMantaUser(mantaUser);
        if (didLock) {
            writeLock.unlock();
        }
        return this;
    }

    @Override
    public TransactionalConfigContext setMantaKeyId(final String mantaKeyId) {
        final boolean didLock = grabWriteLockIfNotHeldByCurrentThread();
        super.setMantaKeyId(mantaKeyId);
        if (didLock) {
            writeLock.unlock();
        }
        return this;
    }

    @Override
    public TransactionalConfigContext setMantaKeyPath(final String mantaKeyPath) {
        final boolean didLock = grabWriteLockIfNotHeldByCurrentThread();
        super.setMantaKeyPath(mantaKeyPath);
        if (didLock) {
            writeLock.unlock();
        }
        return this;
    }

    @Override
    public TransactionalConfigContext setTimeout(final Integer timeout) {
        final boolean didLock = grabWriteLockIfNotHeldByCurrentThread();
        super.setTimeout(timeout);
        if (didLock) {
            writeLock.unlock();
        }
        return this;
    }

    @Override
    public TransactionalConfigContext setRetries(final Integer retries) {
        final boolean didLock = grabWriteLockIfNotHeldByCurrentThread();
        super.setRetries(retries);
        if (didLock) {
            writeLock.unlock();
        }
        return this;
    }

    @Override
    public TransactionalConfigContext setMaximumConnections(final Integer maxConns) {
        final boolean didLock = grabWriteLockIfNotHeldByCurrentThread();
        super.setMaximumConnections(maxConns);
        if (didLock) {
            writeLock.unlock();
        }
        return this;
    }

    @Override
    public TransactionalConfigContext setPrivateKeyContent(final String privateKeyContent) {
        final boolean didLock = grabWriteLockIfNotHeldByCurrentThread();
        super.setPrivateKeyContent(privateKeyContent);
        if (didLock) {
            writeLock.unlock();
        }
        return this;
    }

    @Override
    public TransactionalConfigContext setPassword(final String password) {
        final boolean didLock = grabWriteLockIfNotHeldByCurrentThread();
        super.setPassword(password);
        if (didLock) {
            writeLock.unlock();
        }
        return this;
    }

    @Override
    public TransactionalConfigContext setHttpBufferSize(final Integer httpBufferSize) {
        final boolean didLock = grabWriteLockIfNotHeldByCurrentThread();
        super.setHttpBufferSize(httpBufferSize);
        if (didLock) {
            writeLock.unlock();
        }
        return this;
    }

    @Override
    public TransactionalConfigContext setHttpsProtocols(final String httpsProtocols) {
        final boolean didLock = grabWriteLockIfNotHeldByCurrentThread();
        super.setHttpsProtocols(httpsProtocols);
        if (didLock) {
            writeLock.unlock();
        }
        return this;
    }

    @Override
    public TransactionalConfigContext setHttpsCipherSuites(final String httpsCipherSuites) {
        final boolean didLock = grabWriteLockIfNotHeldByCurrentThread();
        super.setHttpsCipherSuites(httpsCipherSuites);
        if (didLock) {
            writeLock.unlock();
        }
        return this;
    }

    @Override
    public TransactionalConfigContext setNoAuth(final Boolean noAuth) {
        final boolean didLock = grabWriteLockIfNotHeldByCurrentThread();
        super.setNoAuth(noAuth);
        if (didLock) {
            writeLock.unlock();
        }
        return this;
    }

    @Override
    public TransactionalConfigContext setDisableNativeSignatures(final Boolean disableNativeSignatures) {
        final boolean didLock = grabWriteLockIfNotHeldByCurrentThread();
        super.setDisableNativeSignatures(disableNativeSignatures);
        if (didLock) {
            writeLock.unlock();
        }
        return this;
    }

    @Override
    public TransactionalConfigContext setTcpSocketTimeout(final Integer tcpSocketTimeout) {
        final boolean didLock = grabWriteLockIfNotHeldByCurrentThread();
        super.setTcpSocketTimeout(tcpSocketTimeout);
        if (didLock) {
            writeLock.unlock();
        }
        return this;
    }

    @Override
    public TransactionalConfigContext setConnectionRequestTimeout(final Integer connectionRequestTimeout) {
        final boolean didLock = grabWriteLockIfNotHeldByCurrentThread();
        super.setConnectionRequestTimeout(connectionRequestTimeout);
        if (didLock) {
            writeLock.unlock();
        }
        return this;
    }

    @Override
    public TransactionalConfigContext setVerifyUploads(final Boolean verify) {
        final boolean didLock = grabWriteLockIfNotHeldByCurrentThread();
        super.setVerifyUploads(verify);
        if (didLock) {
            writeLock.unlock();
        }
        return this;
    }

    @Override
    public TransactionalConfigContext setUploadBufferSize(final Integer size) {
        final boolean didLock = grabWriteLockIfNotHeldByCurrentThread();
        super.setUploadBufferSize(size);
        if (didLock) {
            writeLock.unlock();
        }
        return this;
    }

    @Override
    public TransactionalConfigContext setClientEncryptionEnabled(final Boolean clientEncryptionEnabled) {
        final boolean didLock = grabWriteLockIfNotHeldByCurrentThread();
        super.setClientEncryptionEnabled(clientEncryptionEnabled);
        if (didLock) {
            writeLock.unlock();
        }
        return this;
    }

    @Override
    public TransactionalConfigContext setEncryptionKeyId(final String keyId) {
        final boolean didLock = grabWriteLockIfNotHeldByCurrentThread();
        super.setEncryptionKeyId(keyId);
        if (didLock) {
            writeLock.unlock();
        }
        return this;
    }

    @Override
    public TransactionalConfigContext setEncryptionAlgorithm(final String algorithm) {
        final boolean didLock = grabWriteLockIfNotHeldByCurrentThread();
        super.setEncryptionAlgorithm(algorithm);
        if (didLock) {
            writeLock.unlock();
        }
        return this;
    }

    @Override
    public TransactionalConfigContext setPermitUnencryptedDownloads(final Boolean permitUnencryptedDownloads) {
        final boolean didLock = grabWriteLockIfNotHeldByCurrentThread();
        super.setPermitUnencryptedDownloads(permitUnencryptedDownloads);
        if (didLock) {
            writeLock.unlock();
        }
        return this;
    }

    @Override
    public TransactionalConfigContext setEncryptionAuthenticationMode(
            final EncryptionAuthenticationMode encryptionAuthenticationMode) {
        final boolean didLock = grabWriteLockIfNotHeldByCurrentThread();
        super.setEncryptionAuthenticationMode(encryptionAuthenticationMode);
        if (didLock) {
            writeLock.unlock();
        }
        return this;
    }

    @Override
    public TransactionalConfigContext setEncryptionPrivateKeyPath(final String encryptionPrivateKeyPath) {
        final boolean didLock = grabWriteLockIfNotHeldByCurrentThread();
        super.setEncryptionPrivateKeyPath(encryptionPrivateKeyPath);
        if (didLock) {
            writeLock.unlock();
        }
        return this;
    }

    @Override
    public TransactionalConfigContext setEncryptionPrivateKeyBytes(final byte[] encryptionPrivateKeyBytes) {
        final boolean didLock = grabWriteLockIfNotHeldByCurrentThread();
        super.setEncryptionPrivateKeyBytes(encryptionPrivateKeyBytes);
        if (didLock) {
            writeLock.unlock();
        }
        return this;
    }

    private boolean grabWriteLockIfNotHeldByCurrentThread() {
        if (!writeLock.isHeldByCurrentThread()) {
            writeLock.lock();
            return true;
        }

        return false;
    }
}
