package com.joyent.manta.client;

import com.joyent.manta.config.ConfigContext;

public class LazyMantaClientValidationTest
        extends MantaClientValidationTest {

    @Override
    protected MantaClient buildClient(final ConfigContext config) {
        final LazyMantaClient client = new LazyMantaClient();
        client.configure(config);
        return client;
    }

}
