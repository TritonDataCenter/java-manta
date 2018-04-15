/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.config;

import com.joyent.http.signature.Signer;
import com.joyent.http.signature.ThreadLocalSigner;
import com.joyent.http.signature.apache.httpclient.HttpSignatureAuthScheme;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;

import java.security.KeyPair;
import java.util.Objects;

/**
 * Object for tracking configuration changes related to authentication and recreating dependent objects as needed.
 * It combines the users' configuration with the derived runtime objects needed to authenticate requests.
 * objects like the {@link ThreadLocalSigner} which needs careful lifecycle management.
 *
 * As far as users are concerned, this class is just as thread-safe as every other {@link ConfigContext} (i.e. generally
 * not) but we're using a private object as a lock in order to at least synchronize reloads and field updates.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 * @since 3.1.7
 */
public class AuthAwareConfigContext
        extends StandardConfigContext
        implements AutoCloseable {

    /**
     * Private lock object for synchronizing updates to fields and building a derived {@link AuthContext}.
     */
    private final Object lock = new Object();

    /**
     * Atomic reference to objects we provide.
     */
    private volatile AuthContext authContext;

    /**
     * Build an empty AuthAwareConfigContext.
     */
    public AuthAwareConfigContext() {
        this(null);
    }

    /**
     * Build an AuthAwareConfigContext from an existing {@link ConfigContext}.
     *
     * @param config configuration context from which to extract values
     */
    public AuthAwareConfigContext(final ConfigContext config) {
        if (config != null) {
            overwriteWithContext(config);
        }

        reload();
    }

    /**
     * Check the configuration for authentication-related changes. Clean up old authentication objects which might
     * still exist and load new instances.
     *
     * @return this after reload
     */
    @SuppressWarnings("unchecked")
    public AuthAwareConfigContext reload() {
        synchronized (lock) {
            final int newParamsFingerprint = calculateAuthParamsFingerprint(this);

            if (authContext != null) {
                if (authContext.paramsFingerprint == newParamsFingerprint) {
                    return this;
                } else {
                    authContext.signer.clearAll();
                    authContext = null;
                }
            }

            if (BooleanUtils.isNotTrue(noAuth())) {
                authContext = doLoad(newParamsFingerprint);
            }
        }

        return this;
    }

    /**
     * Internal method for updating authentication parameters and derived objects.
     *
     * @param paramsFingerprint identifier for the new AuthContext
     * @return the new {@link AuthContext}
     */
    private AuthContext doLoad(final int paramsFingerprint) {
        if (BooleanUtils.isNotFalse(noAuth())) {
            return null;
        }

        final KeyPair keyPair = new KeyPairFactory(this).createKeyPair();

        final Signer.Builder builder = new Signer.Builder(keyPair);
        if (BooleanUtils.isTrue(disableNativeSignatures())) {
            // DefaultConfigContext#DEFAULT_DISABLE_NATIVE_SIGNATURES is false
            builder.providerCode("stdlib");
        }

        final ThreadLocalSigner signer = new ThreadLocalSigner(builder);

        return new AuthContext(
                paramsFingerprint,
                keyPair,
                signer,
                new UsernamePasswordCredentials(getMantaUser(), null),
                new HttpSignatureAuthScheme(keyPair, signer));
    }

    /**
     * This getter is public as a result of package organization.
     * Users are strongly discouraged from directly interacting with this object.
     *
     * @return the credentials used to sign requests
     */
    public Credentials getCredentials() {
        if (authContext == null) {
            return null;
        }

        return authContext.credentials;
    }

    /**
     * This getter is public as a result of package organization.
     * Users are strongly discouraged from directly interacting with this object.
     *
     * @return the auth scheme which does request signing
     */
    public HttpSignatureAuthScheme getAuthScheme() {
        if (authContext == null) {
            return null;
        }

        return authContext.authScheme;
    }

    /**
     * This getter is public as a result of package organization.
     * Users are strongly discouraged from directly interacting with this object.
     *
     * @return the keypair used to sign requests
     */
    public KeyPair getKeyPair() {
        if (authContext == null) {
            return null;
        }

        return authContext.keyPair;
    }

    /**
     * This getter is public as a result of package organization.
     * Users are strongly discouraged from directly interacting with this object.
     *
     * @return the object used to sign requests
     */
    public ThreadLocalSigner getSigner() {
        if (authContext == null) {
            return null;
        }

        return authContext.signer;
    }

    /**
     * Calculate a hashcode of the currently-used configuration parameters.
     *
     * @param config ConfigContext from which to read authentication parameters
     * @return the computed hashcode
     */
    private static int calculateAuthParamsFingerprint(final ConfigContext config) {
        return Objects.hash(
                config.noAuth(),
                config.disableNativeSignatures(),
                config.getMantaUser(),
                config.getPassword(),
                config.getMantaKeyId(),
                config.getMantaKeyPath(),
                config.getPrivateKeyContent());
    }

    @Override
    public void close() throws Exception {
        if (authContext != null) {
            authContext.signer.clearAll();
        }

        authContext = null;
    }

    /**
     * Class for holding references to bundled authentication objects so they can be swapped out atomically.
     */
    private static final class AuthContext {

        /**
         * (Mostly) unique identifier for the config parameters which produced this AuthContext.
         */
        private final int paramsFingerprint;

        /**
         * Reference to loaded KeyPair.
         */
        private final KeyPair keyPair;

        /**
         * Reference to signing object built from {@link #keyPair}.
         */
        private final ThreadLocalSigner signer;

        /**
         * Credentials object used for authenticating requests.
         */
        private final Credentials credentials;

        /**
         * Strategy object for generating headers when generating authenticated requests.
         */
        private final HttpSignatureAuthScheme authScheme;

        @SuppressWarnings("JavadocMethod")
        private AuthContext(final int paramsFingerprint,
                            final KeyPair keyPair,
                            final ThreadLocalSigner signer,
                            final Credentials credentials,
                            final HttpSignatureAuthScheme authScheme) {
            this.paramsFingerprint = paramsFingerprint;
            this.keyPair = keyPair;
            this.signer = signer;
            this.credentials = credentials;
            this.authScheme = authScheme;
        }
    }

    @Override
    public AuthAwareConfigContext setMantaURL(final String mantaURL) {
        synchronized (lock) {
            super.setMantaURL(mantaURL);
        }

        return this;
    }

    @Override
    public AuthAwareConfigContext setMantaUser(final String mantaUser) {
        synchronized (lock) {
            super.setMantaUser(mantaUser);
        }

        return this;
    }

    @Override
    public AuthAwareConfigContext setMantaKeyId(final String mantaKeyId) {
        synchronized (lock) {
            super.setMantaKeyId(mantaKeyId);
        }

        return this;
    }

    @Override
    public AuthAwareConfigContext setMantaKeyPath(final String mantaKeyPath) {
        synchronized (lock) {
            super.setMantaKeyPath(mantaKeyPath);
        }

        return this;
    }

    @Override
    public AuthAwareConfigContext setMonitoringEnabled(final Boolean monitoring) {
        synchronized (lock) {
            super.setMonitoringEnabled(monitoring);
        }

        return this;
    }

    @Override
    public AuthAwareConfigContext setTimeout(final Integer timeout) {
        synchronized (lock) {
            super.setTimeout(timeout);
        }

        return this;
    }

    @Override
    public AuthAwareConfigContext setRetries(final Integer retries) {
        synchronized (lock) {
            super.setRetries(retries);
        }

        return this;
    }

    @Override
    public AuthAwareConfigContext setMaximumConnections(final Integer maxConns) {
        synchronized (lock) {
            super.setMaximumConnections(maxConns);
        }

        return this;
    }

    @Override
    public AuthAwareConfigContext setPrivateKeyContent(final String privateKeyContent) {
        synchronized (lock) {
            super.setPrivateKeyContent(privateKeyContent);
        }

        return this;
    }

    @Override
    public AuthAwareConfigContext setPassword(final String password) {
        synchronized (lock) {
            super.setPassword(password);
        }

        return this;
    }

    @Override
    public AuthAwareConfigContext setHttpBufferSize(final Integer httpBufferSize) {
        synchronized (lock) {
            super.setHttpBufferSize(httpBufferSize);
        }

        return this;
    }

    @Override
    public AuthAwareConfigContext setHttpsProtocols(final String httpsProtocols) {
        synchronized (lock) {
            super.setHttpsProtocols(httpsProtocols);
        }

        return this;
    }

    @Override
    public AuthAwareConfigContext setHttpsCipherSuites(final String httpsCipherSuites) {
        synchronized (lock) {
            super.setHttpsCipherSuites(httpsCipherSuites);
        }

        return this;
    }

    @Override
    public AuthAwareConfigContext setNoAuth(final Boolean noAuth) {
        synchronized (lock) {
            super.setNoAuth(noAuth);
        }

        return this;
    }

    @Override
    public AuthAwareConfigContext setDisableNativeSignatures(final Boolean disableNativeSignatures) {
        synchronized (lock) {
            super.setDisableNativeSignatures(disableNativeSignatures);
        }

        return this;
    }

    @Override
    public AuthAwareConfigContext setTcpSocketTimeout(final Integer tcpSocketTimeout) {
        synchronized (lock) {
            super.setTcpSocketTimeout(tcpSocketTimeout);
        }

        return this;
    }

    @Override
    public AuthAwareConfigContext setConnectionRequestTimeout(final Integer connectionRequestTimeout) {
        synchronized (lock) {
            super.setConnectionRequestTimeout(connectionRequestTimeout);
        }

        return this;
    }

    @Override
    public AuthAwareConfigContext setVerifyUploads(final Boolean verify) {
        synchronized (lock) {
            super.setVerifyUploads(verify);
        }

        return this;
    }

    @Override
    public AuthAwareConfigContext setUploadBufferSize(final Integer size) {
        synchronized (lock) {
            super.setUploadBufferSize(size);
        }

        return this;
    }

    @Override
    public AuthAwareConfigContext setClientEncryptionEnabled(final Boolean clientEncryptionEnabled) {
        synchronized (lock) {
            super.setClientEncryptionEnabled(clientEncryptionEnabled);
        }

        return this;
    }

    @Override
    public AuthAwareConfigContext setEncryptionKeyId(final String keyId) {
        synchronized (lock) {
            super.setEncryptionKeyId(keyId);
        }

        return this;
    }

    @Override
    public AuthAwareConfigContext setEncryptionAlgorithm(final String algorithm) {
        synchronized (lock) {
            super.setEncryptionAlgorithm(algorithm);
        }

        return this;
    }

    @Override
    public AuthAwareConfigContext setPermitUnencryptedDownloads(final Boolean permitUnencryptedDownloads) {
        synchronized (lock) {
            super.setPermitUnencryptedDownloads(permitUnencryptedDownloads);
        }

        return this;
    }

    @Override
    public AuthAwareConfigContext setEncryptionAuthenticationMode(
            final EncryptionAuthenticationMode encryptionAuthenticationMode) {
        synchronized (lock) {
            super.setEncryptionAuthenticationMode(encryptionAuthenticationMode);
        }

        return this;
    }

    @Override
    public AuthAwareConfigContext setEncryptionPrivateKeyPath(final String encryptionPrivateKeyPath) {
        synchronized (lock) {
            super.setEncryptionPrivateKeyPath(encryptionPrivateKeyPath);
        }

        return this;
    }

    @Override
    public AuthAwareConfigContext setEncryptionPrivateKeyBytes(final byte[] encryptionPrivateKeyBytes) {
        synchronized (lock) {
            super.setEncryptionPrivateKeyBytes(encryptionPrivateKeyBytes);
        }

        return this;
    }
}
