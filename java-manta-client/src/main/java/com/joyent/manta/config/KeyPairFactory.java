/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.config;

import com.joyent.http.signature.Signer;
import com.joyent.http.signature.ThreadLocalSigner;
import com.joyent.manta.exception.ConfigurationException;
import org.apache.commons.lang3.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.KeyPair;

import static org.apache.commons.lang3.BooleanUtils.toBoolean;

/**
 * Factory class for generating {@link KeyPair} instances based on passed
 * configuration.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class KeyPairFactory {
    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(KeyPairFactory.class);

    /**
     * Manta configuration object.
     */
    private final ConfigContext config;

    /**
     * Creates a new instance of the KeyPair factory.
     * @param config configuration to use when generating KeyPair objects
     */
    public KeyPairFactory(final ConfigContext config) {
        this.config = config;
    }

    /**
     * Creates a {@link KeyPair} object based on the factory's configuration.
     * @return an encryption key pair
     */
    public KeyPair createKeyPair() {
        final KeyPair keyPair;
        final String password = config.getPassword();
        final String keyPath = config.getMantaKeyPath();
        final ThreadLocalSigner threadLocalSigner;

        if (config.disableNativeSignatures() == null) {
            threadLocalSigner = new ThreadLocalSigner();
        } else {
            threadLocalSigner = new ThreadLocalSigner(!config.disableNativeSignatures());
        }

        final Signer signer = threadLocalSigner.get();

        if (LOGGER.isDebugEnabled()) {
            final boolean nativeGmp = toBoolean(System.getProperty("native.jnagmp"));
            final String enabled = BooleanUtils.toString(nativeGmp, "enabled", "disabled");
            LOGGER.debug("Native GMP is {}", enabled);
        }

        if (signer == null) {
            final String msg = "Error getting signer instance from thread local";
            throw new NullPointerException(msg);
        }

        try {
            if (keyPath != null) {
                keyPair = signer.getKeyPair(new File(keyPath).toPath());
            } else {
                final char[] charPassword;

                if (password != null) {
                    charPassword = password.toCharArray();
                } else {
                    charPassword = null;
                }

                final String privateKeyContent = config.getPrivateKeyContent();

                if (privateKeyContent == null) {
                    String msg = "Private key content setting must be set if "
                            + "key file path is not set";
                    ConfigurationException exception = new ConfigurationException(msg);
                    exception.setContextValue("config", config);
                    throw exception;
                }

                keyPair = signer.getKeyPair(privateKeyContent, charPassword);
            }
        } catch (IOException e) {
            String msg = String.format("Unable to read key files from path: %s",
                    keyPath);
            throw new ConfigurationException(msg, e);
        } finally {
            threadLocalSigner.remove();
        }

        return keyPair;
    }
}
