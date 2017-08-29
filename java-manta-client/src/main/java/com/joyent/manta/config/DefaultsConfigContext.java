/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.config;

import com.joyent.manta.client.crypto.AesCtrCipherDetails;
import com.joyent.manta.client.crypto.SupportedCipherDetails;

import java.io.File;

/**
 * {@link ConfigContext} implementation that outputs nothing but the default
 * values for all of the configuration settings.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class DefaultsConfigContext implements ConfigContext {
    /**
     * The default Manta service endpoint - a public cloud endpoint.
     */
    public static final String DEFAULT_MANTA_URL = "https://us-east.manta.joyent.com:443";

    /**
     * The default timeout for accessing the Manta service.
     */
    public static final int DEFAULT_HTTP_TIMEOUT = 20 * 1000;

    /**
     * The default number of times to retry failed requests.
     */
    public static final int DEFAULT_HTTP_RETRIES = 3;

    /**
     * The default number of maximum connections to allow to the Manta API.
     */
    public static final int DEFAULT_MAX_CONNS = 24;

    /**
     * We assume the default rsa key in the user's home directory.
     */
    public static final String MANTA_KEY_PATH;

    /**
     * The size of the internal socket buffer used to buffer data
     * while receiving / transmitting HTTP messages.
     */
    public static final int DEFAULT_HTTP_BUFFER_SIZE = 4096;

    /**
     * Default TLS protocols.
     */
    public static final String DEFAULT_HTTPS_PROTOCOLS = "TLSv1.2";

    /**
     * By default we verify that checksums on all uploads.
     */
    public static final boolean DEFAULT_VERIFY_UPLOADS = true;

    /**
     * By default we don't allow downloading unencrypted data when using
     * client-side encryption.
     */
    public static final boolean DEFAULT_PERMIT_UNENCRYPTED_DOWNLOADS = false;

    /**
     * Default client-side encryption cipher algorithm.
     */
    public static final SupportedCipherDetails DEFAULT_CIPHER =
            AesCtrCipherDetails.INSTANCE_128_BIT;

    /**
     * Default TLS cipher suites.
     */
    public static final String DEFAULT_HTTPS_CIPHERS =
            "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,"
          + "TLS_RSA_WITH_AES_128_GCM_SHA256,"
          + "TLS_RSA_WITH_AES_256_CBC_SHA256,"
          + "TLS_RSA_WITH_AES_128_CBC_SHA256";

    /**
     * HTTP Signatures for authentication are enabled by default.
     */
    public static final boolean DEFAULT_NO_AUTH = false;

    /**
     * Usage of native extensions for http signatures is enabled by default.
     */
    public static final boolean DEFAULT_DISABLE_NATIVE_SIGNATURES = false;

    /**
     * Default number of milliseconds to wait for a TCP socket's connection to
     * timeout.
     */
    public static final int DEFAULT_TCP_SOCKET_TIMEOUT = 10 * 1000;

    /**
     * Default size of pre-streaming upload buffer (16K).
     */
    public static final int DEFAULT_UPLOAD_BUFFER_SIZE = 16_384;

    static {
        // Don't even bother setting a default key path if it doesn't exist
        String defaultKeyPath = String.format("%s/.ssh/id_rsa",
                System.getProperty("user.home"));
        File privateKeyFile = new File(defaultKeyPath);

        if (privateKeyFile.exists() && privateKeyFile.canRead()) {
            MANTA_KEY_PATH = defaultKeyPath;
        } else {
            MANTA_KEY_PATH = null;
        }
    }

    /**
     * Creates a new instance with all of the defaults assigned to the beans
     * defined in {@link ConfigContext}.
     */
    public DefaultsConfigContext() {
    }

    @Override
    public String getMantaURL() {
        return DEFAULT_MANTA_URL;
    }

    @Override
    public String getMantaUser() {
        return null;
    }

    @Override
    public String getMantaKeyId() {
        return null;
    }

    @Override
    public String getMantaKeyPath() {
        return MANTA_KEY_PATH;
    }

    @Override
    public String getPrivateKeyContent() {
        return null;
    }

    @Override
    public String getPassword() {
        return null;
    }

    @Override
    public Integer getTimeout() {
        return DEFAULT_HTTP_TIMEOUT;
    }

    @Override
    public Integer getRetries() {
        return DEFAULT_HTTP_RETRIES;
    }

    @Override
    public Integer getMaximumConnections() {
        return DEFAULT_MAX_CONNS;
    }

    @Override
    public Integer getHttpBufferSize() {
        return DEFAULT_HTTP_BUFFER_SIZE;
    }

    @Override
    public String getHttpsProtocols() {
        return DEFAULT_HTTPS_PROTOCOLS;
    }

    @Override
    public String getHttpsCipherSuites() {
        return DEFAULT_HTTPS_CIPHERS;
    }

    @Override
    public Boolean noAuth() {
        return DEFAULT_NO_AUTH;
    }

    @Override
    public Boolean disableNativeSignatures() {
        return DEFAULT_DISABLE_NATIVE_SIGNATURES;
    }

    @Override
    public Integer getTcpSocketTimeout() {
        return DEFAULT_TCP_SOCKET_TIMEOUT;
    }

    @Override
    public Boolean verifyUploads() {
        return DEFAULT_VERIFY_UPLOADS;
    }

    @Override
    public Integer getUploadBufferSize() {
        return DEFAULT_UPLOAD_BUFFER_SIZE;
    }

    @Override
    public Boolean isClientEncryptionEnabled() {
        return false;
    }

    @Override
    public String getEncryptionKeyId() {
        return null;
    }

    @Override
    public String getEncryptionAlgorithm() {
        return DEFAULT_CIPHER.getCipherAlgorithm();
    }

    @Override
    public Boolean permitUnencryptedDownloads() {
        return DEFAULT_PERMIT_UNENCRYPTED_DOWNLOADS;
    }

    @Override
    public EncryptionAuthenticationMode getEncryptionAuthenticationMode() {
        return EncryptionAuthenticationMode.DEFAULT_MODE;
    }

    @Override
    public String getEncryptionPrivateKeyPath() {
        return null;
    }

    @Override
    public byte[] getEncryptionPrivateKeyBytes() {
        return null;
    }

    @Override
    public String getMantaHomeDirectory() {
        return ConfigContext.deriveHomeDirectoryFromUser(getMantaUser());
    }

    @Override
    public String toString() {
        return ConfigContext.toString(this);
    }
}
