/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.config.BaseChainedConfigContext;
import com.joyent.manta.config.StandardConfigContext;
import com.joyent.manta.config.TestConfigContext;
import com.joyent.manta.http.HttpHelper;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.security.KeyPair;
import java.util.concurrent.TimeUnit;

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
        HttpHelper userHttpHelper = client.getHttpHelper();
        HttpHelper userHttpHelperSpy = wrapInSpy(client, "lazyHttpHelper");

        config.setMantaUser("admin");
        client.reload();
        HttpHelper adminHttpHelper = client.getHttpHelper();

        Assert.assertNotSame(userHttpHelper, adminHttpHelper);
        HttpHelper adminHttpHelperSpy = wrapInSpy(client, "lazyHttpHelper");

        // previous httpHelper should've been closed
        verify(userHttpHelperSpy, times(1)).close();

        client.close();
        verify(adminHttpHelperSpy, times(1)).close();

        // Mockito will leak references to ThreadLocalSigner since it keeps track of every spy created
    }

    // TEST UTILITY METHODS

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
