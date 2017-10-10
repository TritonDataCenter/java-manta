/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.http.EncryptionHttpHelper;
import com.joyent.manta.http.HttpHelper;
import com.joyent.manta.http.MantaApacheHttpClientContext;
import com.joyent.manta.http.MantaConnectionFactory;
import com.joyent.manta.http.MantaConnectionFactoryConfigurator;
import com.joyent.manta.http.StandardHttpHelper;
import org.apache.commons.lang3.BooleanUtils;

import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A lazily-initializing MantaClient. Users are expected to pass a
 * {@link com.joyent.manta.config.SettableConfigContext} and notify the client of changes by
 * calling {@link #reload()}. Relevant derived components will check whether they need to rebuild
 * themselves.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celayac</a>
 * @since 3.1.7
 */
public final class LazyMantaClient extends MantaClient {

    /**
     * Configuration manager concerned with maintaining the {@link java.security.KeyPair} and
     * {@link com.joyent.http.signature.ThreadLocalSigner} objects needed for the {@link HttpHelper}
     * and {@link UriSigner} components.
     */
    private final AuthenticationConfigurator auth;

    /**
     * Object for deeply customizing the internal behavior of the
     * {@link org.apache.http.impl.client.CloseableHttpClient} that is eventually created to back our
     * {@link HttpHelper}. May be null if no such customization is desired.
     */
    private final MantaConnectionFactoryConfigurator connectionConfig;

    /**
     * MBean supervisor.
     */
    private final MantaMBeanSupervisor lazyBeanSupervisor;

    /**
     * Internal component categories.
     */
    private enum  MantaClientComponent {
        /**
         * The {@link AuthenticationConfigurator} and {@link HttpHelper} both derive their state from
         * the <a href="https://github.com/joyent/java-manta/USAGE.md#configuration-sections">
         * Authentication/Authorization Section</a> of the {@link ConfigContext}.
         */
        AUTH,
        /**
         * The {@link HttpHelper} both derives state from both {@link #AUTH} and the
         * the <a href="https://github.com/joyent/java-manta/USAGE.md#configuration-sections">
         * Network Section</a> of the {@link ConfigContext}.
         */
        HTTP,
        /**
         * The {@link UriSigner} both derives state from {@link #AUTH} but is current a separate component.
         * This value might go away if it were to be rolled into the {@link AuthenticationConfigurator}.
         */
        SIGN,
    }

    /**
     * Set of internal components which need to be dynamically reloaded.
     * Calling {@link #reconfigure()} will cause this set to be filled with all values of the enum.
     * Getters will subsequently remove their component from the {@link EnumSet} once the new
     * value has been cached.
     *
     * PERHAPS: this should be wrapped in an {@link AtomicReference}?
     *
     */
    private volatile EnumSet<MantaClientComponent> reload;

    /**
     * The instance of the http helper class used to simplify creating requests.
     */
    private HttpHelper lazyHttpHelper;

    /**
     * Instance used to generate Manta signed URIs.
     */
    private UriSigner lazyUriSigner;

    /**
     * Build a client which may or may not be immediately useable based on the provided configuration.
     * We do not validate this config at all since it might have changed by the time the getters read it.
     *
     * @param config the {@link ConfigContext} to track
     */
    public LazyMantaClient(final ConfigContext config) {
        this(config, null);
    }

    /**
     * Build a client that tracks a {@link ConfigContext} and may or may not include customized internal HttpClient
     * components. If provided the {@link MantaConnectionFactoryConfigurator} will be passed on to a
     * {@link MantaConnectionFactory} when the {@link HttpHelper} is being rebuilt.
     *
     * @param config the {@link ConfigContext} to track
     * @param connectionConfig customized connection objects to use when creating the {@link HttpHelper}, or null
     */
    public LazyMantaClient(final ConfigContext config,
                           final MantaConnectionFactoryConfigurator connectionConfig) {
        this.config = config;
        this.auth = new AuthenticationConfigurator(config);
        this.connectionConfig = connectionConfig;
        this.lazyBeanSupervisor = new MantaMBeanSupervisor();
        this.reload = EnumSet.allOf(MantaClientComponent.class);
    }

    /**
     * Tell the client it needs to start rebuilding components.
     *
     * @throws Exception an exception that might occur as a result of clearing the registered beans
     */
    public void reload() throws Exception {
        reload = EnumSet.allOf(MantaClientComponent.class);
        lazyBeanSupervisor.reset();
    }

    @Override
    String getUrl() {
        return config.getMantaURL();
    }

    @Override
    String getHome() {
        if (reload.contains(MantaClientComponent.AUTH)) {
            auth.reload();
            reload.remove(MantaClientComponent.AUTH);
        }

        return auth.getHome();
    }

    @Override
    HttpHelper getHttpHelper() {
        if (reload.contains(MantaClientComponent.AUTH)
                || reload.contains(MantaClientComponent.HTTP)) {
            clearHttpHelper();
            auth.reload();
            reload.remove(MantaClientComponent.AUTH);
            reload.remove(MantaClientComponent.HTTP);
        }

        if (lazyHttpHelper == null) {
            lazyHttpHelper = buildHttpHelper();
        }

        return lazyHttpHelper;
    }

    @Override
    UriSigner getUriSigner() {
        if (reload.contains(MantaClientComponent.SIGN)) {
            lazyUriSigner = null;
            auth.reload();
            reload.remove(MantaClientComponent.SIGN);
        }

        if (lazyUriSigner == null) {
            lazyUriSigner = new UriSigner(config, auth.getKeyPair(), auth.getSigner());
        }

        return lazyUriSigner;
    }

    // INTERNAL STATE MANAGEMENT

    /**
     * Clean up the existing {@link HttpHelper} so a new one can be created. Sets the helper reference
     * to {@code null} so that the {@link AuthenticationConfigurator}'s
     * {@link com.joyent.http.signature.ThreadLocalSigner} can be garbage-collected.
     */
    private void clearHttpHelper() {
        if (lazyHttpHelper == null) {
            return;
        }

        try {
            lazyHttpHelper.close();
            lazyHttpHelper = null;
        } catch (final Exception e) {
            throw new MantaLazyConfigurationException("Error occurred while closing existing HttpHelper", e);
        }
    }

    /**
     * Prepare a new {@link HttpHelper}, potentially passing a provided {@link MantaConnectionFactoryConfigurator}.
     * Also registers the {@link MantaConnectionFactory} with {@link #lazyBeanSupervisor}.
     *
     * @return the {@link HttpHelper} to used based on new configuration
     */
    @SuppressWarnings("AvoidInlineConditionals")
    private HttpHelper buildHttpHelper() {
        final MantaConnectionFactory connectionFactory = connectionConfig != null
                ? new MantaConnectionFactory(config, auth.getKeyPair(), auth.getSigner(), connectionConfig)
                : new MantaConnectionFactory(config, auth.getKeyPair(), auth.getSigner());
        final MantaApacheHttpClientContext connectionContext = new MantaApacheHttpClientContext(connectionFactory);

        lazyBeanSupervisor.expose(connectionFactory);

        if (BooleanUtils.isTrue(config.isClientEncryptionEnabled())) {
            return new EncryptionHttpHelper(connectionContext, config);
        }

        return new StandardHttpHelper(connectionContext, config);
    }
}
