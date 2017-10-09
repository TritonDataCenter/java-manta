/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.joyent.http.signature.Signer;
import com.joyent.http.signature.ThreadLocalSigner;
import com.joyent.http.signature.apache.httpclient.HttpSignatureRequestInterceptor;
import com.joyent.manta.config.BaseChainedConfigContext;
import com.joyent.manta.config.TestConfigContext;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.util.reflection.Whitebox;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.security.KeyPair;
import java.util.LinkedList;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@Test
public class MantaConnectionFactoryTest {

    @Mock
    private HttpClientConnectionManager manager;

    @Mock
    private HttpClientBuilder builder;

    private BaseChainedConfigContext config;

    private ImmutablePair<KeyPair, ThreadLocalSigner> authContext;

    private MantaConnectionFactory connectionFactory;

    @BeforeMethod
    public void setUp() throws IOException {
        MockitoAnnotations.initMocks(this);
        builder.setConnectionManager(manager);

        final ImmutablePair<KeyPair, BaseChainedConfigContext> keypairAndConfig = TestConfigContext.generateKeyPairBackedConfig();
        final ThreadLocalSigner signer = new ThreadLocalSigner(new Signer.Builder(keypairAndConfig.left).providerCode("stdlib"));
        authContext = new ImmutablePair<>(keypairAndConfig.left, signer);
        config = keypairAndConfig.right
                .setMantaURL("https://localhost")
                .setMantaUser("user");
    }

    @AfterMethod
    public void tearDown() throws IOException {
        Mockito.validateMockitoUsage();

        if (connectionFactory != null) {
            connectionFactory.close();
        }

        // clear references to signer
        connectionFactory = null;   // references builder which has auth interceptor
        authContext = null;         // directly references signer
        builder = null;             // might have an auth interceptor
    }

    /*
        Note: we can't do normal verification here using Mockito.verify() because HttpClientBuilder's
        setter methods are final. Mockito reports:

        this error might show up because you verify either of: final/private/equals()/hashCode() methods.
        Those methods *cannot* be stubbed/verified.
     */

    public void willShutdownCreatedConnectionManager() throws IOException {
        connectionFactory = new MantaConnectionFactory(config, authContext.left, authContext.right);
        final CloseableHttpClient client = connectionFactory.createConnection();

        connectionFactory.close();

        final IllegalStateException shutdownException = Assert.expectThrows(IllegalStateException.class, () ->
                client.execute(new HttpGet("http://localhost")));

        Assert.assertTrue(shutdownException.getMessage().contains("Connection pool shut down"));
    }

    public void willSkipConnectionManagerShutdownWhenFactoryClosesAndManagerIsShared() throws IOException {
        final MantaConnectionFactoryConfigurator conf = new MantaConnectionFactoryConfigurator(builder);
        connectionFactory = new MantaConnectionFactory(config, authContext.left, authContext.right, conf);

        connectionFactory.close();
        verify(manager, never()).shutdown();
    }

    public void willNotChangeConfiguredManager() throws IOException {
        final MantaConnectionFactoryConfigurator conf = new MantaConnectionFactoryConfigurator(builder);
        connectionFactory = new MantaConnectionFactory(config, authContext.left, authContext.right, conf);

        final HttpClientConnectionManager configuredManager =
                (HttpClientConnectionManager) Whitebox.getInternalState(builder, "connManager");

        Assert.assertEquals(manager, configuredManager);
    }

    @SuppressWarnings("unchecked")
    public void willAttachAuthInterceptorToInternallyConstructedClient() throws IOException {
        connectionFactory = new MantaConnectionFactory(config, authContext.left, authContext.right);

        final HttpClientBuilder internallyCreatedBuilder =
                (HttpClientBuilder) Whitebox.getInternalState(connectionFactory, "httpClientBuilder");

        final LinkedList<HttpRequestInterceptor> interceptors =
                (LinkedList<HttpRequestInterceptor>) Whitebox.getInternalState(internallyCreatedBuilder, "requestLast");

        boolean foundAuthInterceptor = false;
        for (final HttpRequestInterceptor requestInterceptor : interceptors) {
            if (requestInterceptor instanceof HttpSignatureRequestInterceptor) {
                foundAuthInterceptor = true;
                break;
            }
        }

        Assert.assertTrue(foundAuthInterceptor);
        connectionFactory.close();
    }

    @SuppressWarnings("unchecked")
    public void willAttachAuthInterceptorToProvidedClient() {
        final MantaConnectionFactoryConfigurator conf = new MantaConnectionFactoryConfigurator(builder);
        connectionFactory = new MantaConnectionFactory(config, authContext.left, authContext.right, conf);

        final LinkedList<HttpRequestInterceptor> interceptors =
                (LinkedList<HttpRequestInterceptor>) Whitebox.getInternalState(builder, "requestLast");

        boolean foundAuthInterceptor = false;
        for (final HttpRequestInterceptor requestInterceptor : interceptors) {
            if (requestInterceptor instanceof HttpSignatureRequestInterceptor) {
                foundAuthInterceptor = true;
                break;
            }
        }

        Assert.assertTrue(foundAuthInterceptor);
    }

    public void willActuallyDisableRetriesOnInternallyConstructedBuilderWhenSetToZero() throws IOException {
        config.setRetries(0);

        connectionFactory = new MantaConnectionFactory(config, authContext.left, authContext.right);

        final HttpClientBuilder internallyCreatedBuilder =
                (HttpClientBuilder) Whitebox.getInternalState(connectionFactory, "httpClientBuilder");

        Assert.assertTrue((Boolean) Whitebox.getInternalState(internallyCreatedBuilder, "automaticRetriesDisabled"));
    }

    public void willActuallyDisableRetriesOnProvidedBuilderWhenSetToZero() throws IOException {
        config.setRetries(0);

        final MantaConnectionFactoryConfigurator conf = new MantaConnectionFactoryConfigurator(builder);
        connectionFactory = new MantaConnectionFactory(config, authContext.left, authContext.right, conf);

        Assert.assertTrue((Boolean) Whitebox.getInternalState(builder, "automaticRetriesDisabled"));
    }

    public void willAttachInternalRetryHandlersToInternalBuilder() throws IOException {
        config.setRetries(1);

        connectionFactory = new MantaConnectionFactory(config, authContext.left, authContext.right);

        final HttpClientBuilder internallyCreatedBuilder =
                (HttpClientBuilder) Whitebox.getInternalState(connectionFactory, "httpClientBuilder");

        final Object retryHandler = Whitebox.getInternalState(internallyCreatedBuilder, "retryHandler");
        final Object serviceUnavailStrategy = Whitebox.getInternalState(internallyCreatedBuilder, "serviceUnavailStrategy");

        Assert.assertTrue(retryHandler instanceof MantaHttpRequestRetryHandler);
        Assert.assertTrue(serviceUnavailStrategy instanceof MantaServiceUnavailableRetryStrategy);
    }

    public void willAttachInternalRetryHandlersToProvidedBuilder() throws IOException {
        config.setRetries(1);

        final MantaConnectionFactoryConfigurator conf = new MantaConnectionFactoryConfigurator(builder);
        connectionFactory = new MantaConnectionFactory(config, authContext.left, authContext.right, conf);

        final Object retryHandler = Whitebox.getInternalState(builder, "retryHandler");
        final Object serviceUnavailStrategy = Whitebox.getInternalState(builder, "serviceUnavailStrategy");


        Assert.assertTrue(retryHandler instanceof MantaHttpRequestRetryHandler);
        Assert.assertTrue(serviceUnavailStrategy instanceof MantaServiceUnavailableRetryStrategy);
    }
}
