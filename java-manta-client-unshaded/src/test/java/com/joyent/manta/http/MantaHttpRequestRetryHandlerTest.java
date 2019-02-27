package com.joyent.manta.http;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.joyent.manta.config.MantaClientMetricConfiguration;
import org.apache.http.client.protocol.HttpClientContext;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public class MantaHttpRequestRetryHandlerTest {

    private static final MetricFilter FILTER_RETRY_METRIC = (name, metric) -> name.equals("retries");

    public void indicatesShouldRetryOnGenericIOException() {
        final MantaHttpRequestRetryHandler retryHandler = new MantaHttpRequestRetryHandler(1);

        assertTrue(retryHandler.retryRequest(new IOException("something went wrong"), 1, new HttpClientContext()));
    }

    public void createsRetriesMetricInRegistry() {
        final MetricRegistry registry = new MetricRegistry();
        final UUID id = new UUID(0L, 1L);

        /* Registration of the registry is a side-effect of creating a new handler.
         * In order for this test to run successfully, this instance *must* be created. */
        new MantaHttpRequestRetryHandler(0, new MantaClientMetricConfiguration(id, registry));

        final Collection<Meter> meters = registry.getMeters(FILTER_RETRY_METRIC).values();
        assertEquals(meters.size(), 1);
    }

    @Test(dependsOnMethods = {"indicatesShouldRetryOnGenericIOException", "createsRetriesMetricInRegistry"})
    public void recordsRetryMetric() {
        final MetricRegistry registry = new MetricRegistry();
        final MantaHttpRequestRetryHandler retryHandler = new MantaHttpRequestRetryHandler(1, new MantaClientMetricConfiguration(UUID.randomUUID(), registry));

        final Optional<Meter> maybeMeter = registry.getMeters(FILTER_RETRY_METRIC).values().stream().findFirst();
        assertTrue(maybeMeter.isPresent());

        assertEquals(maybeMeter.get().getCount(), 0, "meter should have zero samples");

        retryHandler.retryRequest(new IOException("something went wrong"), 1, new HttpClientContext());
        assertEquals(maybeMeter.get().getCount(), 1, "meter should have one sample");

        retryHandler.retryRequest(new IOException("something else went wrong"), 1, new HttpClientContext());
        assertEquals(maybeMeter.get().getCount(), 2, "meter should have two samples");
    }
}
