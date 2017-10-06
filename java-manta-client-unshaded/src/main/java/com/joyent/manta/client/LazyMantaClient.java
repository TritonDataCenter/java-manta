package com.joyent.manta.client;

import com.joyent.manta.config.AuthenticationConfigurator;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.http.EncryptionHttpHelper;
import com.joyent.manta.http.HttpHelper;
import com.joyent.manta.http.MantaApacheHttpClientContext;
import com.joyent.manta.http.MantaConnectionFactory;
import com.joyent.manta.http.MantaConnectionFactoryConfigurator;
import com.joyent.manta.http.StandardHttpHelper;
import org.apache.commons.lang3.BooleanUtils;

import java.util.EnumSet;

public class LazyMantaClient extends MantaClient {

    private final Object configureLock = new Object();

    private final AuthenticationConfigurator auth;

    private final MantaConnectionFactoryConfigurator connectionConfig;

    private enum  MantaClientComponent {
        AUTH,
        HTTP,
        SIGNING,
    }

    private volatile EnumSet<MantaClientComponent> reconfigureRequired;

    /**
     * The instance of the http helper class used to simplify creating requests.
     */
    private HttpHelper lazyHttpHelper;

    /**
     * Instance used to generate Manta signed URIs.
     */
    private UriSigner lazyUriSigner;

    /**
     * MBean supervisor.
     */
    private MantaMBeanSupervisor lazyBeanSupervisor;

    public LazyMantaClient(final ConfigContext config) {
        this(config, null);
    }

    public LazyMantaClient(final ConfigContext config,
                           final MantaConnectionFactoryConfigurator connectionConfig) {
        this.config = config;
        this.auth = new AuthenticationConfigurator();
        this.connectionConfig = connectionConfig;
        this.lazyBeanSupervisor = new MantaMBeanSupervisor();
        this.reconfigureRequired = EnumSet.allOf(MantaClientComponent.class);
    }


    public void reconfigure() throws Exception {
        reconfigureRequired = EnumSet.allOf(MantaClientComponent.class);
        lazyBeanSupervisor.reset();
    }

    String getHome() {
        if (!reconfigureRequired.isEmpty() && reconfigureRequired.contains(MantaClientComponent.AUTH)) {
            auth.configure(config);
            reconfigureRequired.remove(MantaClientComponent.AUTH);
        }

        return auth.getHome();
    }

    @Override
    HttpHelper getHttpHelper() {
        if (!reconfigureRequired.isEmpty() && reconfigureRequired.contains(MantaClientComponent.HTTP)) {
            clearHttpHelper();
            auth.configure(config);
            reconfigureRequired.remove(MantaClientComponent.HTTP);
        }

        if (lazyHttpHelper == null) {
            lazyHttpHelper = buildHttpHelper();
        }

        return lazyHttpHelper;
    }

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

    @Override
    UriSigner getUriSigner() {
        if (!reconfigureRequired.isEmpty() && reconfigureRequired.contains(MantaClientComponent.AUTH)) {
            lazyUriSigner = null;
            auth.configure(config);
            reconfigureRequired.remove(MantaClientComponent.AUTH);
        }

        if (lazyUriSigner == null) {
            lazyUriSigner = new UriSigner(config, auth.getKeyPair(), auth.getSigner());
        }

        return lazyUriSigner;
    }
}
