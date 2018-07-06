package com.joyent.manta.util;

import com.joyent.manta.config.MantaClientMetricConfiguration;

public interface MetricsAware {
    MantaClientMetricConfiguration getMetricConfig();
}
