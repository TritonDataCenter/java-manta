/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.http.signature.Signer;
import com.joyent.http.signature.ThreadLocalSigner;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.KeyPairFactory;
import com.joyent.manta.config.TestConfigContext;
import com.joyent.manta.http.MantaConnectionFactory;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseFactory;
import org.apache.http.NoHttpResponseException;
import org.apache.http.ParseException;
import org.apache.http.config.MessageConstraints;
import org.apache.http.conn.HttpConnectionFactory;
import org.apache.http.conn.ManagedHttpClientConnection;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.DefaultHttpResponseFactory;
import org.apache.http.impl.conn.ManagedHttpClientConnectionFactory;
import org.apache.http.impl.io.DefaultHttpRequestWriterFactory;
import org.apache.http.impl.io.DefaultHttpResponseParser;
import org.apache.http.impl.io.DefaultHttpResponseParserFactory;
import org.apache.http.io.HttpMessageParser;
import org.apache.http.io.SessionInputBuffer;
import org.apache.http.message.BasicLineParser;
import org.apache.http.message.LineParser;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.security.KeyPair;
import java.util.UUID;

@Test
public class MantaClientConnectionFailuresIT {
    private ConfigContext config;
    private int retryAttempts;

    @BeforeClass()
    @Parameters({"manta.url", "manta.user", "manta.key_path", "manta.key_id", "manta.timeout", "manta.retries"})
    public void beforeClass(@Optional String mantaUrl,
                            @Optional String mantaUser,
                            @Optional String mantaKeyPath,
                            @Optional String mantaKeyId,
                            @Optional Integer mantaTimeout,
                            @Optional InputStream retries) {

        // Let TestNG configuration take precedence over environment variables
        this.config = new TestConfigContext(
                mantaUrl, mantaUser, mantaKeyPath, mantaKeyId, mantaTimeout);

        // We start at -1 one because the first try doesn't count
        this.retryAttempts = -1;
    }

    private class NoHttpResponseDefaultHttpResponseParser extends DefaultHttpResponseParser {
        public NoHttpResponseDefaultHttpResponseParser(SessionInputBuffer buffer, LineParser lineParser, HttpResponseFactory responseFactory, MessageConstraints constraints) {
            super(buffer, lineParser, responseFactory, constraints);
        }

        @Override
        protected HttpResponse parseHead(SessionInputBuffer sessionBuffer) throws IOException, HttpException, ParseException {
            retryAttempts++;
            throw new NoHttpResponseException("simulating server failure to respond");
        }
    }

    private class NoHttpResponseDefaultHttpResponseParserFactory extends DefaultHttpResponseParserFactory {
        private final LineParser lineParser;
        private final HttpResponseFactory responseFactory;

        public NoHttpResponseDefaultHttpResponseParserFactory(final LineParser lineParser,
                                                              final HttpResponseFactory responseFactory) {
            super(lineParser, responseFactory);
            this.lineParser = lineParser != null ? lineParser : BasicLineParser.INSTANCE;
            this.responseFactory = responseFactory != null ? responseFactory
                    : DefaultHttpResponseFactory.INSTANCE;
        }

        public NoHttpResponseDefaultHttpResponseParserFactory() {
            this(null, null);
        }

        @Override
        public HttpMessageParser<HttpResponse> create(final SessionInputBuffer buffer,
                                                      final MessageConstraints constraints) {
            return new NoHttpResponseDefaultHttpResponseParser(buffer, lineParser, responseFactory, constraints);
        }
    }

    private class NoHttpResponseMantaConnectionFactory extends MantaConnectionFactory {
        public NoHttpResponseMantaConnectionFactory(ConfigContext config, KeyPair keyPair, ThreadLocalSigner signer) {
            super(config, keyPair, signer);
        }

        @Override
        protected HttpConnectionFactory<HttpRoute, ManagedHttpClientConnection> buildHttpConnectionFactory() {
            return new ManagedHttpClientConnectionFactory(
                    new DefaultHttpRequestWriterFactory(),
                    new NoHttpResponseDefaultHttpResponseParserFactory());
        }
    }

    public void canRetryOnNoHttpResponseException() throws IOException {
        final KeyPairFactory keyPairFactory = new KeyPairFactory(config);
        final KeyPair keyPair = keyPairFactory.createKeyPair();
        final ThreadLocalSigner signer = new ThreadLocalSigner(new Signer.Builder(keyPair));
        MantaClient mantaClient = new MantaClient(config, keyPair,
                                                  new NoHttpResponseMantaConnectionFactory(config, keyPair, signer),
                                                  signer);
        String testPathPrefix = String.format("%s/stor/%s/",
                config.getMantaHomeDirectory(), UUID.randomUUID());

        boolean thrown = false;
        try {
            mantaClient.head(testPathPrefix);
        } catch (NoHttpResponseException e) {
            thrown = true;
        } finally {
            mantaClient.closeWithWarning();
        }

        Assert.assertTrue(thrown, "NoHttpResponseException was never thrown");
        Assert.assertEquals(retryAttempts, config.getRetries().intValue());
    }
}
