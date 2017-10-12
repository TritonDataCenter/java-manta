/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.http.signature.apache.httpclient.HttpSignatureRequestInterceptor;
import com.joyent.manta.config.BaseChainedConfigContext;
import com.joyent.manta.config.StandardConfigContext;
import com.joyent.manta.config.TestConfigContext;
import com.joyent.manta.http.HttpHelper;
import com.joyent.manta.http.MantaConnectionFactoryConfigurator;
import com.joyent.manta.http.RequestIdInterceptor;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.execchain.ProtocolExec;
import org.apache.http.protocol.ImmutableHttpProcessor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.security.KeyPair;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Optional;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@Test
public class LazyMantaClientTest {

    private BaseChainedConfigContext config;

    private KeyPair keyPair;

    @BeforeMethod
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        final ImmutablePair<KeyPair, BaseChainedConfigContext> keyPairAndConfig =
                TestConfigContext.generateKeyPairBackedConfig();
        keyPair = keyPairAndConfig.left;
        config = keyPairAndConfig.right;
    }

    @AfterMethod
    public void tearDown() throws IOException {
        Mockito.validateMockitoUsage();
        Mockito.reset();
        keyPair = null;
        config = null;
    }

    @AfterClass
    public void afterClass() throws Exception {
        TimeUnit.MINUTES.sleep(10);
    }

    public void canBeInstantiatedAndClosedWithInvalidConfiguration() throws Exception {
        // required parameters are null
        config.setMantaURL(null);
        config.setMantaUser(null);

        new LazyMantaClient(new StandardConfigContext()).close();
    }

    public void willThrowValidatingNPEsWhenFailingToLazilyInitializeComponents() {
        // auth is definitely enabled but we haven't provided a user
        config.setMantaUser(null);
        config.setNoAuth(false);
        final MantaClient client = new LazyMantaClient(config);

        Assert.assertThrows(NullPointerException.class, client::getHttpHelper);
        Assert.assertThrows(NullPointerException.class, client::getUriSigner);
    }

    public void canReloadSignerWhenAuthChanges() throws Exception {
        config.setMantaUser("user");
        final LazyMantaClient client = new LazyMantaClient(config);
        final UriSigner userUriSigner = client.getUriSigner();
        final KeyPair userLoadedKeyPair = (KeyPair) FieldUtils.readField(userUriSigner, "keyPair", true);

        differentKeyPairsSameContent(keyPair, userLoadedKeyPair);

        config.setMantaUser("admin");
        client.reload();
        final UriSigner adminUriSigner = client.getUriSigner();

        Assert.assertNotSame(userUriSigner, adminUriSigner);
        final KeyPair adminLoadedKeyPair = (KeyPair) FieldUtils.readField(client.getUriSigner(), "keyPair", true);

        differentKeyPairsSameContent(keyPair, adminLoadedKeyPair);
        client.close();
    }

    public void canReloadHttpHelperWhenAuthChanges() throws Exception {
        config.setMantaUser("user");
        final LazyMantaClient client = new LazyMantaClient(config);
        final HttpHelper userHttpHelper = client.getHttpHelper();
        final HttpHelper userHttpHelperSpy = wrapInSpy(client, "lazyHttpHelper");

        config.setMantaUser("admin");
        client.reload();
        final HttpHelper adminHttpHelper = client.getHttpHelper();

        // previous httpHelper should've been closed
        verify(userHttpHelperSpy, times(1)).close();

        Assert.assertNotSame(userHttpHelper, adminHttpHelper);
        final HttpHelper adminHttpHelperSpy = wrapInSpy(client, "lazyHttpHelper");

        client.close();
        verify(adminHttpHelperSpy, times(1)).close();

        // Mockito will leak references to ThreadLocalSigner since it keeps track of every spy created
    }

    public void canReloadHttpHelperCleanlyWithInternallyCreatedConnection() throws Exception {
        config.setMantaUser("user");
        final LazyMantaClient client = new LazyMantaClient(config);
        final HttpHelper authedHttpHelper = client.getHttpHelper();
        final HttpRequestInterceptor[] interceptorsIncludingAuth =
                findRequestInterceptors(authedHttpHelper.getConnectionContext().getHttpClient());

        ensureExpectedInterceptorsPresent(interceptorsIncludingAuth, true);

        config.setNoAuth(true);
        client.reload();
        final HttpHelper unauthedHttpHelper = client.getHttpHelper();
        final HttpRequestInterceptor[] interceptorsWithoutAuth =
                findRequestInterceptors(unauthedHttpHelper.getConnectionContext().getHttpClient());

        ensureExpectedInterceptorsPresent(interceptorsWithoutAuth, false);
    }

    public void canReloadHttpHelperCleanlyWithProvidedConnectionConfigurator() throws Exception {
        config.setMantaUser("user");

        final HttpClientBuilder builder = HttpClientBuilder.create();
        final LazyMantaClient client = new LazyMantaClient(config, new MantaConnectionFactoryConfigurator(builder));
        final HttpHelper authedHttpHelper = client.getHttpHelper();
        final HttpRequestInterceptor[] interceptorsIncludingAuth =
                findRequestInterceptors(authedHttpHelper.getConnectionContext().getHttpClient());

        ensureExpectedInterceptorsPresent(interceptorsIncludingAuth, true);

        config.setNoAuth(true);
        client.reload();
        final HttpHelper unauthedHttpHelper = client.getHttpHelper();
        final HttpRequestInterceptor[] interceptorsWithoutAuth =
                findRequestInterceptors(unauthedHttpHelper.getConnectionContext().getHttpClient());

        ensureExpectedInterceptorsPresent(interceptorsWithoutAuth, false);
    }

    public void canReloadHttpHelperRepeatedlyWithoutLeakingInterceptors() throws Exception {
        final HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
        httpClientBuilder.addInterceptorFirst(
                (HttpRequestInterceptor) (request, context) -> System.out.println("req start"));
        httpClientBuilder.addInterceptorLast(
                (HttpRequestInterceptor) (request, context) -> System.out.println("req end"));
        httpClientBuilder.addInterceptorFirst(
                (HttpResponseInterceptor) (request, context) -> System.out.println("res start"));
        httpClientBuilder.addInterceptorLast(
                (HttpResponseInterceptor) (request, context) -> System.out.println("res end"));

        int interceptorCount = countInterceptors(httpClientBuilder);

        Assert.assertEquals(interceptorCount, 4);

        final LazyMantaClient client =
                new LazyMantaClient(config, new MantaConnectionFactoryConfigurator(httpClientBuilder));

        // reload the client repeatedly while changing the username and toggling authentication
        for (int i = 0; i < 10; i++) {
            final boolean disableAuth = i % 2 == 0;

            config.setNoAuth(disableAuth);
            config.setMantaUser("user" + i);
            client.reload();
            client.getHttpHelper();

            Assert.assertEquals(interceptorCount, countInterceptors(httpClientBuilder));

            final CloseableHttpClient httpClient = client.getHttpHelper().getConnectionContext().getHttpClient();
            ensureExpectedInterceptorsPresent(findRequestInterceptors(httpClient), !disableAuth);
        }
    }

    private int countInterceptors(final HttpClientBuilder httpClientBuilder) throws IllegalAccessException {
        int total = 0;

        for (String fieldname : new String[]{
                "requestFirst",
                "requestLast",
                "responseFirst",
                "responseFirst",
        }) {
            LinkedList interceptorList = (LinkedList) FieldUtils.readField(httpClientBuilder, fieldname, true);
            if (interceptorList != null) {
                total += interceptorList.size();
            }
        }

        return total;
    }

    // TEST UTILITY METHODS

    private HttpRequestInterceptor[] findRequestInterceptors(final CloseableHttpClient httpClient)
            throws Exception {
        int maxDepthRemaining = 10;
        Object requestExecutor = FieldUtils.readField(httpClient, "execChain", true);

        while (requestExecutor != null
                && !(requestExecutor instanceof ProtocolExec)
                && 0 < --maxDepthRemaining) {
            requestExecutor = FieldUtils.readField(requestExecutor, "requestExecutor", true);
        }

        if (!(requestExecutor instanceof ProtocolExec)) {
            throw new Exception("Failed to find ProtocolExec in search of HttpProcessor");
        }

        final ImmutableHttpProcessor httpProcessor =
                (ImmutableHttpProcessor) FieldUtils.readField(requestExecutor, "httpProcessor", true);
        final HttpRequestInterceptor[] requestInterceptors =
                (HttpRequestInterceptor[]) FieldUtils.readField(httpProcessor, "requestInterceptors", true);

        return requestInterceptors;
    }

    /**
     * Verify that interceptors are not carried over between reloads. Looking at the implementation of
     * {@link org.apache.http.protocol.HttpProcessorBuilder}'s usage of a deduplicating
     * {@link org.apache.http.protocol.ChainBuilder} means that the shallow clone in
     * {@link MantaConnectionFactoryConfigurator} is not necessary for the correctness of the
     * {@link CloseableHttpClient} that is ultimately built. On the other hand, every time a configuration
     * change occurs, another interceptor would be added to the provided {@link HttpClientBuilder} and
     * the memory usage of that builder would grown indefinitely. See {@link canReloadHttpHelperRepeatedlyWithoutLeakingInterceptors()}
     *
     * @param interceptors
     * @param authEnabled
     */
    private void ensureExpectedInterceptorsPresent(final HttpRequestInterceptor[] interceptors,
                                                   final boolean authEnabled) {
        final HashSet<Class> expectedInterceptorClasses = new HashSet<>();
        expectedInterceptorClasses.add(RequestIdInterceptor.class);

        final HashSet<Class> forbiddenInterceptorClasses = new HashSet<>();

        if (authEnabled) {
            expectedInterceptorClasses.add(HttpSignatureRequestInterceptor.class);
        } else {
            forbiddenInterceptorClasses.add(HttpSignatureRequestInterceptor.class);
        }

        final HashSet<Class> seenInterceptorClasses = new HashSet<>();

        for (HttpRequestInterceptor i : interceptors) {
            final Optional<Class> expectedMatched = expectedInterceptorClasses.stream()
                    .filter((eC) -> eC.isInstance(i))
                    .findFirst();

            final Optional<Class> forbiddenMatched = forbiddenInterceptorClasses.stream()
                    .filter((fC) -> fC.isInstance(i))
                    .findFirst();

            Assert.assertFalse(forbiddenMatched.isPresent(), "Forbidden interceptor discovered");

            if (expectedMatched.isPresent()) {
                Assert.assertFalse(
                        seenInterceptorClasses.contains(expectedMatched.get()),
                        "Duplicate interceptor found: " + expectedMatched.get().getCanonicalName());

                seenInterceptorClasses.add(expectedMatched.get());
            }
        }

        Assert.assertEquals(
                expectedInterceptorClasses.size(), seenInterceptorClasses.size(),
                String.format(
                        "Expected interceptors did not match discovered interceptors:" +
                                "%sExpected: [%s]" +
                                "%sDiscovered: [%s]",
                        System.lineSeparator(),
                        Arrays.toString(expectedInterceptorClasses.toArray()),
                        System.lineSeparator(),
                        Arrays.toString(seenInterceptorClasses.toArray())));
    }

    @SuppressWarnings("unchecked")
    private static <T> T wrapInSpy(final Object o,
                                   final String fieldname) throws ReflectiveOperationException {
        final Object field = FieldUtils.readField(o, fieldname, true);
        final T spy = (T) Mockito.spy(field);
        FieldUtils.writeField(o, fieldname, spy, true);
        return spy;
    }

    private void differentKeyPairsSameContent(final KeyPair firstKeyPair, final KeyPair secondKeyPair) {
        // different instances
        Assert.assertNotSame(firstKeyPair, secondKeyPair);

        // same key
        AssertJUnit.assertArrayEquals(
                firstKeyPair.getPrivate().getEncoded(),
                secondKeyPair.getPrivate().getEncoded());
        AssertJUnit.assertArrayEquals(
                firstKeyPair.getPublic().getEncoded(),
                secondKeyPair.getPublic().getEncoded());
    }

}
