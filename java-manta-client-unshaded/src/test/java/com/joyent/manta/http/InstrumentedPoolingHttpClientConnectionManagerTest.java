package com.joyent.manta.http;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.joyent.manta.http.InstrumentedPoolingHttpClientConnectionManager.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Test
public class InstrumentedPoolingHttpClientConnectionManagerTest {

    public void createsPoolStatsMetrics() {
        final MetricRegistry metricRegistry = new MetricRegistry();
        final Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
            .register("http", PlainConnectionSocketFactory.getSocketFactory())
            .register("https", SSLConnectionSocketFactory.getSocketFactory())
            .build();

        final PoolingHttpClientConnectionManager connManager = new InstrumentedPoolingHttpClientConnectionManager(
                    metricRegistry,
                    socketFactoryRegistry,
                    null,
                    null,
                    null,
                    -1,
                    TimeUnit.MILLISECONDS);

        final int maxConns = 5;
        connManager.setMaxTotal(maxConns);

        @SuppressWarnings("unchecked")
        final Map<String, Gauge> gauges = metricRegistry.getGauges();

        // pool stats are present
        assertNotNull(gauges.get(METRIC_NAME_CONNECTIONS_AVAILABLE));
        assertNotNull(gauges.get(METRIC_NAME_CONNECTIONS_LEASED));
        assertNotNull(gauges.get(METRIC_NAME_CONNECTIONS_MAX));
        assertNotNull(gauges.get(METRIC_NAME_CONNECTIONS_PENDING));

        // max = expectedMax
        assertEquals(gauges.get(METRIC_NAME_CONNECTIONS_MAX).getValue(), maxConns);

        // no one is in the pool and no connections have been requested
        assertEquals(gauges.get(METRIC_NAME_CONNECTIONS_AVAILABLE).getValue(), 0);
        assertEquals(gauges.get(METRIC_NAME_CONNECTIONS_LEASED).getValue(), 0);
        assertEquals(gauges.get(METRIC_NAME_CONNECTIONS_PENDING).getValue(), 0);

        connManager.shutdown();
    }
}
