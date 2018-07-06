/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import com.joyent.manta.config.MantaClientMetricConfiguration;

/**
 * Marker interface indicating that a class may carry a metric configuration (or encapsulate something that does).
 * Returning null is acceptable and indicates that metric collection is disabled.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 * @since 3.2.3
 */
public interface MetricsAware {

    /**
     * Get the configuration.
     *
     * @return null or the configuration
     */
    MantaClientMetricConfiguration getMetricConfig();
}
