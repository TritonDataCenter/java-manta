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
import com.joyent.manta.config.BaseChainedConfigContext;
import com.joyent.manta.config.TestConfigContext;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.security.KeyPair;

import static com.joyent.manta.util.UnitTestConstants.UNIT_TEST_URL;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@Test
public class MantaConnectionFactoryTest {

    @Mock
    private HttpClientConnectionManager manager;

    @Mock
    private HttpClientBuilder builder;

    private BaseChainedConfigContext config;

    private MantaConnectionFactory connectionFactory;

    @BeforeMethod
    public void setUp() throws IOException {
        MockitoAnnotations.initMocks(this);
        builder.setConnectionManager(manager);

        final ImmutablePair<KeyPair, BaseChainedConfigContext> keypairAndConfig = TestConfigContext.generateKeyPairBackedConfig();
        final ThreadLocalSigner signer = new ThreadLocalSigner(new Signer.Builder(keypairAndConfig.left).providerCode("stdlib"));
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
        builder = null;             // might have an auth interceptor
    }

    /*
        Note: we can't do normal verification here using Mockito.verify() because HttpClientBuilder's
        setter methods are final. Mockito reports:

        this error might show up because you verify either of: final/private/equals()/hashCode() methods.
        Those methods *cannot* be stubbed/verified.
     */

    public void willShutdownCreatedConnectionManager() throws IOException {
        connectionFactory = new MantaConnectionFactory(config);
        final CloseableHttpClient client = connectionFactory.createConnection();

        connectionFactory.close();

        final IllegalStateException shutdownException = Assert.expectThrows(IllegalStateException.class, () ->
                client.execute(new HttpGet(UNIT_TEST_URL)));

        Assert.assertTrue(shutdownException.getMessage().contains("Connection pool shut down"));
    }

    public void willSkipConnectionManagerShutdownWhenFactoryClosesAndManagerIsShared() throws IOException {
        final MantaConnectionFactoryConfigurator conf = new MantaConnectionFactoryConfigurator(builder);
        connectionFactory = new MantaConnectionFactory(config, conf);

        connectionFactory.close();
        verify(manager, never()).shutdown();
    }

    public void willConfigureClientToUseProvidedManager() throws IOException, ReflectiveOperationException {
        final MantaConnectionFactoryConfigurator conf = new MantaConnectionFactoryConfigurator(builder);
        connectionFactory = new MantaConnectionFactory(config, conf);

        final HttpClientConnectionManager configuredManager =
                (HttpClientConnectionManager) FieldUtils.readField(builder, "connManager", true);

        Assert.assertEquals(manager, configuredManager);
    }

    public void willActuallyDisableRetriesOnInternallyConstructedBuilderWhenSetToZero()
            throws IOException, ReflectiveOperationException {
        config.setRetries(0);

        connectionFactory = new MantaConnectionFactory(config);

        final HttpClientBuilder internallyCreatedBuilder =
                (HttpClientBuilder) FieldUtils.readField(connectionFactory, "httpClientBuilder", true);

        Assert.assertTrue((Boolean) FieldUtils.readField(internallyCreatedBuilder, "automaticRetriesDisabled", true));
    }

    public void willActuallyDisableRetriesOnProvidedBuilderWhenSetToZero() throws IOException, ReflectiveOperationException {
        config.setRetries(0);

        final MantaConnectionFactoryConfigurator conf = new MantaConnectionFactoryConfigurator(builder);
        connectionFactory = new MantaConnectionFactory(config, conf);

        final Object factoryInternalBuilder = FieldUtils.readField(connectionFactory, "httpClientBuilder", true);
        Assert.assertTrue((Boolean) FieldUtils.readField(factoryInternalBuilder, "automaticRetriesDisabled", true));
    }

    public void willAttachInternalRetryHandlersToInternalBuilder() throws IOException, ReflectiveOperationException {
        config.setRetries(1);

        connectionFactory = new MantaConnectionFactory(config);

        final HttpClientBuilder internallyCreatedBuilder =
                (HttpClientBuilder) FieldUtils.readField(connectionFactory, "httpClientBuilder", true);

        final Object retryHandler = FieldUtils.readField(internallyCreatedBuilder, "retryHandler", true);
        final Object serviceUnavailStrategy = FieldUtils.readField(internallyCreatedBuilder, "serviceUnavailStrategy", true);

        Assert.assertTrue(retryHandler instanceof MantaHttpRequestRetryHandler);
        Assert.assertTrue(serviceUnavailStrategy instanceof MantaServiceUnavailableRetryStrategy);
    }

    public void willAttachInternalRetryHandlersToProvidedBuilder() throws IOException, ReflectiveOperationException {
        config.setRetries(1);

        final MantaConnectionFactoryConfigurator conf = new MantaConnectionFactoryConfigurator(builder);
        connectionFactory = new MantaConnectionFactory(config, conf);

        final Object factoryInternalBuilder = FieldUtils.readField(connectionFactory, "httpClientBuilder", true);
        final Object retryHandler = FieldUtils.readField(factoryInternalBuilder, "retryHandler", true);
        final Object serviceUnavailStrategy = FieldUtils.readField(factoryInternalBuilder, "serviceUnavailStrategy", true);

        Assert.assertTrue(retryHandler instanceof MantaHttpRequestRetryHandler);
        Assert.assertTrue(serviceUnavailStrategy instanceof MantaServiceUnavailableRetryStrategy);
    }
}
