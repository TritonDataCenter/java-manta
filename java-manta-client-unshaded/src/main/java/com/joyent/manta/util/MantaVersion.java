/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import java.time.Instant;

/**
 * Utility class providing the current version of the SDK.
 */
public final class MantaVersion {
    /**
     * No public constructor because only static methods are exposed.
     */
    private MantaVersion() {
    }

    /**
     * Release version of the SDK.
     */
    public static final String VERSION = "3.1.7-SNAPSHOT";

    /**
     * Minimum version of client-side encryption supported.
     */
    public static final int CLIENT_SIDE_ENCRYPTION_MIN_VERSION = 1;

    /**
     * Maximum version of client-side encryption supported.
     */
    public static final int CLIENT_SIDE_ENCRYPTION_MAX_VERSION = 1;

    /**
     * Release date of the SDK.
     */
    public static final Instant DATE = Instant.now();
}
