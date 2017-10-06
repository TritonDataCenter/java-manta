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

/**
 * A lazily-initializing MantaClient. Users are expected to pass a
 * {@link com.joyent.manta.config.SettableConfigContext} and notify the client of changes by
 * calling {@link #reconfigure()}. Relevant derived components will check whether they need to rebuild
 * themselves.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celayac</a>
 * @since 3.1.7
 */
public class LazyMantaClient extends MantaClient {

    private final AuthenticationConfigurator auth;

    private final MantaConnectionFactoryConfigurator connectionConfig;

    /**
     * MBean supervisor.
     */
    private final MantaMBeanSupervisor lazyBeanSupervisor;

    private enum  MantaClientComponent {
        AUTH,
        HTTP,
        SIGN,
    }

    private volatile EnumSet<MantaClientComponent> reconfigure;

    /**
     * The instance of the http helper class used to simplify creating requests.
     */
    private HttpHelper lazyHttpHelper;

    /**
     * Instance used to generate Manta signed URIs.
     */
    private UriSigner lazyUriSigner;

    public LazyMantaClient(final ConfigContext config) {
        this(config, null);
    }

    public LazyMantaClient(final ConfigContext config,
                           final MantaConnectionFactoryConfigurator connectionConfig) {
        this.config = config;
        this.auth = new AuthenticationConfigurator();
        this.connectionConfig = connectionConfig;
        this.lazyBeanSupervisor = new MantaMBeanSupervisor();
        this.reconfigure = EnumSet.allOf(MantaClientComponent.class);
    }

    /**
     * Tell the client it needs to start rebuilding components
     *
     * @throws Exception
     */
    public void reconfigure() throws Exception {
        reconfigure = EnumSet.allOf(MantaClientComponent.class);
        lazyBeanSupervisor.reset();
    }

    @Override
    String getUrl() {
        return config.getMantaURL();
    }

    @Override
    String getHome() {
        if (reconfigure.contains(MantaClientComponent.AUTH)) {
            auth.configure(config);
            reconfigure.remove(MantaClientComponent.AUTH);
        }

        return auth.getHome();
    }

    @Override
    HttpHelper getHttpHelper() {
        if (reconfigure.contains(MantaClientComponent.AUTH)
                || reconfigure.contains(MantaClientComponent.HTTP)) {
            clearHttpHelper();
            auth.configure(config);
            reconfigure.remove(MantaClientComponent.AUTH);
            reconfigure.remove(MantaClientComponent.HTTP);
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
        if (reconfigure.contains(MantaClientComponent.SIGN)) {
            lazyUriSigner = null;
            auth.configure(config);
            reconfigure.remove(MantaClientComponent.SIGN);
        }

        if (lazyUriSigner == null) {
            lazyUriSigner = new UriSigner(config, auth.getKeyPair(), auth.getSigner());
        }

        return lazyUriSigner;
    }
}
