/*
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
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
    public static final String VERSION = "3.0.0-SNAPSHOT";

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
