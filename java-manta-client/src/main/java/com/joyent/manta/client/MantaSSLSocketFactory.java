/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.exception.MantaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import static com.joyent.manta.client.MantaUtils.fromCsv;

/**
 * Custom {@link org.apache.http.conn.ssl.SSLSocketFactory} implementation that consumes Manta
 * configuration and enforces the selection of protocols and ciphers.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
@SuppressWarnings("deprecation")
public class MantaSSLSocketFactory extends org.apache.http.conn.ssl.SSLSocketFactory {
    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(MantaSSLSocketFactory.class);

    /**
     * Set of supported TLS protocols.
     */
    private final Set<String> supportedProtocols;

    /**
     * Set of supported TLS cipher suites.
     */
    private final Set<String> supportedCipherSuites;

    /**
     * Creates a new instance using the Manta configuration parameters.
     * @param config configuration context containing SSL config params
     */
    public MantaSSLSocketFactory(final ConfigContext config) {
        super(buildContext(), getSystemSocketFactory().getHostnameVerifier());

        if (config.getHttpsProtocols() != null) {
            this.supportedProtocols = new LinkedHashSet<>(fromCsv(config.getHttpsProtocols()));
        } else {
            this.supportedProtocols = Collections.emptySet();
        }

        if (config.getHttpsCipherSuites() != null) {
            this.supportedCipherSuites = new LinkedHashSet<>(fromCsv(config.getHttpsCipherSuites()));
        } else {
            this.supportedCipherSuites = Collections.emptySet();
        }
    }

    /**
     * @return reference to SSL Context
     */
    private static SSLContext buildContext() {
        try {
            return SSLContext.getDefault();
        } catch (NoSuchAlgorithmException e) {
            throw new MantaException(e);
        }
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
                throw new MantaException(msg, e);
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
