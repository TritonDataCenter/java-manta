/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.http.signature.Signer;
import com.joyent.http.signature.ThreadLocalSigner;
import com.joyent.http.signature.apache.httpclient.HttpSignatureAuthScheme;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.KeyPairFactory;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;

import java.security.KeyPair;
import java.util.Objects;

/**
 * Object for tracking configuration changes related to authentication and recreating dependent objects as needed.
 * This class lives in the client package because so that its getters can be package-private. It is
 * designed to ease client initialization and acts as a bridge configuration values and runtime authentication
 * objects like the {@link ThreadLocalSigner} which needs careful lifecycle management.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celayac</a>
 * @since 3.1.7
 */
public class AuthenticationConfigurator implements AutoCloseable {

    /**
     * The {@link ConfigContext} to track for changes.
     */
    private final ConfigContext config;

    /**
     * Hashcode of last observed authentication-related configuration values.
     */
    private int parametersFingerprint;

    /**
     * Reference to loaded KeyPair.
     */
    private KeyPair keyPair;

    /**
     * Reference to signing object built from {@link #keyPair}.
     */
    private ThreadLocalSigner signer;

    /**
     * Credentials object used for authenticating requests.
     */
    private Credentials credentials;

    /**
     * Strategy object for generating headers when generating authenticated requests.
     */
    private HttpSignatureAuthScheme authScheme;

    /**
     * Build an AuthenticationConfigurator from an existing {@link ConfigContext}.
     *
     * @param config configuration context from which to extract values
     */
    public AuthenticationConfigurator(final ConfigContext config) {
        this.config = config;
        reload();
    }

    /**
     * Check the configuration for authentication-related changes. Clean up old authentication objects which might
     * still exist and load new instances.
     *
     * @return this after reload
     */
    public synchronized AuthenticationConfigurator reload() {
        final int newFingerprint = calculateAuthParamsFingerprint(config);

        if (newFingerprint == parametersFingerprint) {
            return this;
        }

        if (signer != null) {
            signer.clearAll();
        }
        signer = null;
        keyPair = null;
        credentials = null;
        authScheme = null;

        if (BooleanUtils.isNotTrue(config.noAuth())) {
            doLoad();
        }

        parametersFingerprint = newFingerprint;

        return this;
    }

    /**
     * Internal method for updating authentication parameters and derived objects.
     */
    private void doLoad() {
        if (BooleanUtils.isNotFalse(config.noAuth())) {
            return;
        }

        keyPair = new KeyPairFactory(config).createKeyPair();

        final Signer.Builder builder = new Signer.Builder(keyPair);
        if (BooleanUtils.isTrue(config.disableNativeSignatures())) {
            // DefaultConfigContext#DEFAULT_DISABLE_NATIVE_SIGNATURES is false
            builder.providerCode("stdlib");
        }

        signer = new ThreadLocalSigner(builder);
        credentials = new UsernamePasswordCredentials(config.getMantaUser(), null);
        authScheme = new HttpSignatureAuthScheme(keyPair, signer);
    }

    public String getURL() {
        return config.getMantaURL();
    }

    public String getUsername() {
        return config.getMantaUser();
    }

    /**
     * Whether or not the user KeyPair (and signer) are loaded.
     *
     * @return current authentication setting
     */
    public Boolean isAuthenticationDisabled() {
        return config.noAuth();
    }

    public Credentials getCredentials() {
        return credentials;
    }

    public HttpSignatureAuthScheme getAuthScheme() {
        return authScheme;
    }

    ConfigContext getContext() {
        return config;
    }

    KeyPair getKeyPair() {
        return keyPair;
    }

    ThreadLocalSigner getSigner() {
        return signer;
    }

    /**
     * Calculate a hashcode of the currently-used configuration parameters.
     *
     * @param config ConfigContext from which to read authentication parameters
     * @return the computed hashcode
     */
    private static int calculateAuthParamsFingerprint(final ConfigContext config) {
        return Objects.hash(
                config.getPassword(),
                config.getMantaKeyId(),
                config.getMantaKeyPath(),
                config.getPrivateKeyContent(),
                config.disableNativeSignatures(),
                config.noAuth());
    }

    @Override
    public void close() throws Exception {
        keyPair = null;

        if (signer != null) {
            signer.clearAll();
        }

        signer = null;
    }
}
