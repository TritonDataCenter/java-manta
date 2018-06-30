/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.apache.http.config.Registry;
import org.apache.http.conn.DnsResolver;
import org.apache.http.conn.HttpConnectionFactory;
import org.apache.http.conn.ManagedHttpClientConnection;
import org.apache.http.conn.SchemePortResolver;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.util.concurrent.TimeUnit;

/**
 * Custom reimplementation of <a href="http://
 * metrics.dropwizard.io/4.0.0/apidocs/com/codahale/metrics/httpclient/InstrumentedHttpClientConnectionManager.html>
 * InstrumentedHttpClientConnectionManager</a> with more structured metric names (since we put categories before
 * specific metric names) and omitting the "org.apache.http.conn.HttpClientConnectionManager" section.
 *
 * Metrics are named "connections-$CLASSIFICATION"
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 * @since 3.2.2
 */
@SuppressWarnings("checkstyle:JavaDocVariable")
class InstrumentedPoolingHttpClientConnectionManager extends PoolingHttpClientConnectionManager {

    static final String METRIC_NAME_CONNECTIONS_AVAILABLE = "connections-available";
    static final String METRIC_NAME_CONNECTIONS_LEASED = "connections-leased";
    static final String METRIC_NAME_CONNECTIONS_MAX = "connections-max";
    static final String METRIC_NAME_CONNECTIONS_PENDING = "connections-pending";

    /**
     * Registry used to track metrics. Never null.
     */
    private final MetricRegistry metricRegistry;

    InstrumentedPoolingHttpClientConnectionManager(
            final MetricRegistry metricRegistry,
            final Registry<ConnectionSocketFactory> socketFactoryRegistry,
            final HttpConnectionFactory<HttpRoute, ManagedHttpClientConnection> connFactory,
            final SchemePortResolver schemePortResolver,
            final DnsResolver dnsResolver,
            final long timeToLive,
            final TimeUnit tunit
    ) {
        super(socketFactoryRegistry, connFactory, schemePortResolver, dnsResolver, timeToLive, tunit);
        this.metricRegistry = metricRegistry;

        // Just like PoolStatsMBean, getTotalStats aquires a lock
        this.metricRegistry.register(METRIC_NAME_CONNECTIONS_AVAILABLE,
                (Gauge<Integer>) () -> getTotalStats().getAvailable());
        this.metricRegistry.register(METRIC_NAME_CONNECTIONS_LEASED,
                (Gauge<Integer>) () -> getTotalStats().getLeased());
        this.metricRegistry.register(METRIC_NAME_CONNECTIONS_MAX,
                (Gauge<Integer>) () -> getTotalStats().getMax());
        this.metricRegistry.register(METRIC_NAME_CONNECTIONS_PENDING,
                (Gauge<Integer>) () -> getTotalStats().getPending());
    }

    @Override
    public void shutdown() {
        super.shutdown();
        metricRegistry.remove(METRIC_NAME_CONNECTIONS_AVAILABLE);
        metricRegistry.remove(METRIC_NAME_CONNECTIONS_LEASED);
        metricRegistry.remove(METRIC_NAME_CONNECTIONS_MAX);
        metricRegistry.remove(METRIC_NAME_CONNECTIONS_PENDING);
    }
}
