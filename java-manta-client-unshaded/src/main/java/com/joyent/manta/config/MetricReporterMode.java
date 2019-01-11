/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.config;

/**
 * Enum specifying the available metric reporting modes.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 * @since 3.1.9, 3.2.2
 */
public enum MetricReporterMode {
    /**
     * No metrics will be collected or reported.
     */
    DISABLED,

    /**
     * Report metrics through JMX (as MBeans).
     */
    JMX,

    /**
     * Report metrics through SLF4J (as logs).
     */
    SLF4J;

    /**
     * The default metric reporting mode is to disable metric reporting.
     */
    public static final MetricReporterMode DEFAULT = DISABLED;
}
