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
import java.util.concurrent.atomic.AtomicReference;

/**
 * Object for tracking configuration changes related to authentication and recreating dependent objects as needed.
 * It combines the users' configuration with the derived runtime objects needed to authenticate requests.
 * objects like the {@link ThreadLocalSigner} which needs careful lifecycle management.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celayac</a>
 * @since 3.1.7
 */
public class AuthAwareConfigContext
        extends StandardConfigContext
        implements AutoCloseable {

    /**
     * Hashcode of last observed authentication-related configuration values.
     */
    private volatile int parametersFingerprint;

    /**
     * Atomic reference to objects we provide.
     */
    private final AtomicReference<AuthContext> authContextRef = new AtomicReference<>();

    /**
     * Build an AuthAwareConfigContext from an existing {@link ConfigContext}.
     *
     * @param config configuration context from which to extract values
     */
    public AuthAwareConfigContext(final ConfigContext config) {
        overwriteWithContext(config);
        reload();
    }

    /**
     * Check the configuration for authentication-related changes. Clean up old authentication objects which might
     * still exist and load new instances.
     *
     * @return this after reload
     */
    public synchronized AuthAwareConfigContext reload() {
        final int newFingerprint = calculateAuthParamsFingerprint(this);

        if (newFingerprint == parametersFingerprint) {
            return this;
        }

        final AuthContext authContext = authContextRef.get();

        if (authContext != null) {
            final ThreadLocalSigner signer = authContext.signer;
            if (signer != null) {
                signer.clearAll();
            }
            authContextRef.set(null);
        }

        final AuthContext newAuthContext;
        if (BooleanUtils.isTrue(noAuth())) {
            newAuthContext = null;
        } else {
            newAuthContext = doLoad();
        }

        authContextRef.set(newAuthContext);
        parametersFingerprint = newFingerprint;

        return this;
    }

    /**
     * Internal method for updating authentication parameters and derived objects.
     *
     * @return the new {@link AuthContext}
     */
    private AuthContext doLoad() {
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
        final AuthContext authContext = authContextRef.get();

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
        final AuthContext authContext = authContextRef.get();

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
        final AuthContext authContext = authContextRef.get();

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
        final AuthContext authContext = authContextRef.get();

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
                config.getMantaUser(),
                config.getPassword(),
                config.getMantaKeyId(),
                config.getMantaKeyPath(),
                config.getPrivateKeyContent(),
                config.disableNativeSignatures(),
                config.noAuth());
    }

    @Override
    public void close() throws Exception {
        final AuthContext authContext = authContextRef.get();

        if (authContext != null) {
            authContext.signer.clearAll();
        }

        authContextRef.set(null);
    }

    /**
     * Class for holding references to bundled authentication objects so they can be swapped out atomically.
     */
    private static final class AuthContext {

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

        @SuppressWarnings("JavadocMethod")
        private AuthContext(final KeyPair keyPair,
                            final ThreadLocalSigner signer,
                            final Credentials credentials,
                            final HttpSignatureAuthScheme authScheme) {
            this.keyPair = keyPair;
            this.signer = signer;
            this.credentials = credentials;
            this.authScheme = authScheme;
        }
    }
}
