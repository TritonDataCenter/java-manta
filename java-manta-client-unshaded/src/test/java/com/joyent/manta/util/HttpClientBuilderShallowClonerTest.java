/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import com.joyent.manta.http.RequestIdInterceptor;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.HttpClientBuilder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.LinkedList;

import static org.mockito.Mockito.mock;

@Test
public class HttpClientBuilderShallowClonerTest {

    @Mock
    private HttpClientBuilder httpClientBuilder;

    private HttpClientBuilderShallowCloner cloner = new HttpClientBuilderShallowCloner();

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @AfterMethod
    public void tearDown() {
        Mockito.validateMockitoUsage();
        httpClientBuilder = null;
    }

    public void canCloneHttpClientBuilder() throws Exception {
        final HttpHost proxy = new HttpHost("localhost");
        final HttpClientConnectionManager connectionManager = mock(HttpClientConnectionManager.class);
        final HttpRequestRetryHandler retryHandler = mock(HttpRequestRetryHandler.class);

        httpClientBuilder.setProxy(proxy);
        httpClientBuilder.setConnectionManager(connectionManager);
        httpClientBuilder.setRetryHandler(retryHandler);

        final HttpClientBuilder clonedBuilder = cloner.createClone(httpClientBuilder);

        Assert.assertSame(proxy, FieldUtils.readField(clonedBuilder, "proxy", true));
        Assert.assertSame(connectionManager, FieldUtils.readField(clonedBuilder, "connManager", true));
        Assert.assertSame(retryHandler, FieldUtils.readField(clonedBuilder, "retryHandler", true));
    }

    public void canCloneRequestInterceptorLists() throws Exception {
        httpClientBuilder.addInterceptorFirst(new RequestIdInterceptor());
        httpClientBuilder.addInterceptorLast(new RequestIdInterceptor());

        canCloneInterceptorList(new RequestIdInterceptor(), "request");
    }

    public void canCloneResponseInterceptorLists() throws Exception {
        final HttpResponseInterceptor responseInterceptor = (response, context) -> {
        };

        httpClientBuilder.addInterceptorFirst(responseInterceptor);
        httpClientBuilder.addInterceptorLast(responseInterceptor);

        canCloneInterceptorList(responseInterceptor, "response");
    }

    private void canCloneInterceptorList(final Object extraInterceptor, final String interceptAction) throws Exception {
        final LinkedList<?> originalInterceptFirst = (LinkedList<?>) FieldUtils.readField(httpClientBuilder, interceptAction + "First", true);
        final LinkedList<?> originalInterceptLast = (LinkedList<?>) FieldUtils.readField(httpClientBuilder, interceptAction + "Last", true);

        final HttpClientBuilder clonedBuilder = cloner.createClone(httpClientBuilder);

        if (extraInterceptor instanceof HttpRequestInterceptor) {
            clonedBuilder.addInterceptorFirst((HttpRequestInterceptor) extraInterceptor);
            clonedBuilder.addInterceptorLast((HttpRequestInterceptor) extraInterceptor);
        } else if (extraInterceptor instanceof HttpResponseInterceptor) {
            clonedBuilder.addInterceptorFirst((HttpResponseInterceptor) extraInterceptor);
            clonedBuilder.addInterceptorLast((HttpResponseInterceptor) extraInterceptor);
        } else {
            throw new Exception("Unexpected interceptor type: " + extraInterceptor.getClass().getCanonicalName());
        }

        final LinkedList<?> clonedInterceptFirst = (LinkedList<?>) FieldUtils.readField(clonedBuilder, interceptAction + "First", true);
        final LinkedList<?> clonedInterceptLast = (LinkedList<?>) FieldUtils.readField(clonedBuilder, interceptAction + "Last", true);

        Assert.assertEquals(originalInterceptFirst.size(), 1);
        Assert.assertEquals(originalInterceptLast.size(), 1);
        Assert.assertEquals(clonedInterceptFirst.size(), 2);
        Assert.assertEquals(clonedInterceptLast.size(), 2);
    }
}
