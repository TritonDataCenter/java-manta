/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.config;

import com.joyent.http.signature.KeyFingerprinter;
import com.joyent.http.signature.KeyPairLoader;
import com.joyent.manta.exception.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.KeyPair;

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
        final String privateKeyContent = config.getPrivateKeyContent();
        final String keyPath = config.getMantaKeyPath();
        final String password = config.getPassword();

        try {
            if (privateKeyContent != null) {
                final char[] charPassword;
                if (password != null) {
                    charPassword = password.toCharArray();
                } else {
                    charPassword = null;
                }
                keyPair = KeyPairLoader.getKeyPair(privateKeyContent, charPassword);
            } else if (keyPath != null) {
                keyPair = KeyPairLoader.getKeyPair(new File(keyPath).toPath());
            } else {
                String msg = "Private key content setting must be set if "
                    + "key file path is not set";
                ConfigurationException exception = new ConfigurationException(msg);
                exception.setContextValue("config", config);
                throw exception;
            }
        } catch (IOException e) {
            String msg = String.format("Unable to read key files from path: %s",
                    keyPath);
            throw new ConfigurationException(msg, e);
        }

        if (!KeyFingerprinter.verifyFingerprint(keyPair, config.getMantaKeyId())) {
            String msg = String.format("Given fingerprint %s does not match expected key "
                                       + "MD5:%s SHA256:%s",
                                       config.getMantaKeyId(),
                                       KeyFingerprinter.md5Fingerprint(keyPair),
                                       KeyFingerprinter.sha256Fingerprint(keyPair));
            ConfigurationException exception = new ConfigurationException(msg);
            throw exception;
        }

        return keyPair;
    }
}
