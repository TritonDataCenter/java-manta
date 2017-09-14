/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.joyent.manta.util.MantaUtils;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.exception.ConfigurationException;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.ssl.SSLContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Custom {@link SSLConnectionSocketFactory} implementation that consumes Manta
 * configuration and enforces the selection of protocols and ciphers.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class MantaSSLConnectionSocketFactory extends SSLConnectionSocketFactory {
    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(MantaSSLConnectionSocketFactory.class);

    /**
     * Set of supported TLS protocols.
     */
    private final Set<String> supportedProtocols;

    /**
     * Set of supported TLS cipher suites.
     */
    private final Set<String> supportedCipherSuites;

    /**
     * Creates a new instance using the configuration parameters.
     * @param config configuration context containing SSL config params
     */
    public MantaSSLConnectionSocketFactory(final ConfigContext config) {
        super(buildContext(),
              MantaUtils.csv2array(config.getHttpsProtocols()),
              MantaUtils.csv2array(config.getHttpsCipherSuites()),
              getDefaultHostnameVerifier());

        if (config.getHttpsProtocols() != null) {
            this.supportedProtocols = new LinkedHashSet<>(MantaUtils.fromCsv(config.getHttpsProtocols()));
        } else {
            this.supportedProtocols = Collections.emptySet();
        }

        if (config.getHttpsCipherSuites() != null) {
            this.supportedCipherSuites = new LinkedHashSet<>(MantaUtils.fromCsv(config.getHttpsCipherSuites()));
        } else {
            this.supportedCipherSuites = Collections.emptySet();
        }
    }

    /**
     * @return reference to SSL Context
     */
    private static SSLContext buildContext() {
        return SSLContexts.createDefault();
    }

    @Override
    protected void prepareSocket(final SSLSocket socket) throws IOException {
        final Set<String> enabledProtocols = new LinkedHashSet<>(
                Arrays.asList(socket.getEnabledProtocols()));
        final Set<String> enabledCipherSuites = new LinkedHashSet<>(
                Arrays.asList(socket.getEnabledCipherSuites()));

        if (LOG.isDebugEnabled()) {
            LOG.debug("Enabled TLS protocols: {}", MantaUtils.asString(enabledProtocols));
            LOG.debug("Enabled cipher suites: {}", MantaUtils.asString(enabledCipherSuites));
        }

        supportedCipherSuites.retainAll(enabledCipherSuites);

        if (!supportedCipherSuites.isEmpty()) {
            try {
                String[] supportedCiphers = new String[supportedCipherSuites.size()];
                supportedCipherSuites.toArray(supportedCiphers);
                socket.setEnabledCipherSuites(supportedCiphers);
            } catch (IllegalArgumentException e) {
                String msg = String.format("Unsupported encryption provider. Supported providers: %s",
                        MantaUtils.asString(socket.getEnabledCipherSuites()));
                throw new ConfigurationException(msg, e);
            }
        }

        supportedProtocols.retainAll(enabledProtocols);

        if (!supportedProtocols.isEmpty()) {
            String[] supportedProtos = new String[supportedProtocols.size()];
            supportedProtocols.toArray(supportedProtos);
            socket.setEnabledProtocols(supportedProtos);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Supported TLS protocols: {}", MantaUtils.asString(supportedProtocols));
            LOG.debug("Supported cipher suites: {}", MantaUtils.asString(supportedCipherSuites));
        }
    }
}
