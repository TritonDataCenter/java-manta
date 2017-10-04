package com.joyent.manta.http;

import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.HttpClientBuilder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

@Test
public class MantaConnectionFactoryConfiguratorTest {

    @Mock
    private HttpClientConnectionManager manager;

    @Mock
    private HttpClientBuilder builder;

    @BeforeMethod
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);
    }

    public void willValidateInputs() {
        Assert.assertThrows(() -> new MantaConnectionFactoryConfigurator(null, null));
        Assert.assertThrows(() -> new MantaConnectionFactoryConfigurator(manager, null));
        Assert.assertThrows(() -> new MantaConnectionFactoryConfigurator(null, builder));
    }

}