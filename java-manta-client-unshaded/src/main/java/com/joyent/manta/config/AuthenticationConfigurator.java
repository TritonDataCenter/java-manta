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
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.Validate;

import java.security.KeyPair;
import java.util.Objects;

/**
 * Object for tracking configuration changes related to authentication and recreating dependent objects as needed.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celayac</a>
 * @since 3.1.7
 */
public class AuthenticationConfigurator {

    private final Object configureLock = new Object();

    /**
     * Hashcode of last observed authentication-related configuration values.
     */
    private int parametersFingerprint;

    /**
     * Copy of configured username.
     */
    private String username;

    /**
     * Home directory derived from username (which might be a subuser).
     */
    private String home;

    /**
     * Reference to loaded KeyPair.
     */
    private KeyPair keyPair;

    /**
     * Reference to signing object built from {@link #keyPair}.
     */
    private ThreadLocalSigner signer;

    public AuthenticationConfigurator() {
    }

    /**
     * Build an AuthenticationConfigurator from an existing {@link ConfigContext}.
     *
     * @param config configuration context from which to extract values
     */
    public AuthenticationConfigurator(final ConfigContext config) {
        configure(config);
    }

    /**
     * words.
     *
     * @param newConfig the updated (or a completely different) config
     */
    public void configure(final ConfigContext newConfig) {
        Validate.notNull(newConfig, "Configuration must not be null");
        synchronized (configureLock) {
            username = null;
            home = null;
            keyPair = null;
            signer = null;

            final int newFingerprint = calculateAuthParamsFingerprint(newConfig);

            if (newFingerprint == parametersFingerprint) {
                return;
            }

            doConfigure(newConfig);
            parametersFingerprint = newFingerprint;
        }
    }

    /**
     * Internal method for updating authentication parameters and derived objects.
     *
     * @param config ConfigContext to use as a source of data
     */
    private void doConfigure(final ConfigContext config) {
        username = config.getMantaUser();

        if (BooleanUtils.isNotFalse(config.noAuth())) {
            keyPair = null;
            signer = null;
            return;
        }

        keyPair = new KeyPairFactory(config).createKeyPair();

        final Signer.Builder builder = new Signer.Builder(keyPair);
        if (ObjectUtils.firstNonNull(
                config.disableNativeSignatures(),
                DefaultsConfigContext.DEFAULT_DISABLE_NATIVE_SIGNATURES)) {
            builder.providerCode("stdlib");
        }

        signer = new ThreadLocalSigner(builder);
    }

    public String getUsername() {
        return username;
    }

    public String getHome() {
        return home;
    }

    public KeyPair getKeyPair() {
        return keyPair;
    }

    public ThreadLocalSigner getSigner() {
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
                config.getMantaUser(),
                config.getPassword(),
                config.getMantaKeyId(),
                config.getMantaKeyPath(),
                config.noAuth());
    }
}
