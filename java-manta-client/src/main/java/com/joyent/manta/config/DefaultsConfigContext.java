/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.config;

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
     * The default {@link com.google.api.client.http.HttpTransport} implementation to use.
     */
    public static final String DEFAULT_HTTP_TRANSPORT = "ApacheHttpTransport";

    /**
     * Default TLS protocols.
     */
    public static final String DEFAULT_HTTPS_PROTOCOLS = "TLSv1.2";

    /**
     * Default TLS cipher suites.
     */
    public static final String DEFAULT_HTTPS_CIPHERS =
            "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,"
          + "TLS_RSA_WITH_AES_128_GCM_SHA256,"
          + "TLS_RSA_WITH_AES_256_CBC_SHA256,"
          + "TLS_RSA_WITH_AES_128_CBC_SHA256";

    /**
     * Default HTTP signature cache TTL.
     */
    public static final int DEFAULT_SIGNATURE_CACHE_TTL = 0;

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
    public String getHttpTransport() {
        return DEFAULT_HTTP_TRANSPORT;
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
        return false;
    }

    @Override
    public Boolean disableNativeSignatures() {
        return false;
    }

    @Override
    public Integer getSignatureCacheTTL() {
        return DEFAULT_SIGNATURE_CACHE_TTL;
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
