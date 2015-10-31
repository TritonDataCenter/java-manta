/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.config;

import com.joyent.manta.client.MantaClient;

/**
 * Interface representing the configuration properties needed to configure a
 * {@link MantaClient}.
 */
public interface ConfigContext {
    int DEFAULT_HTTP_TIMEOUT = 20 * 1000;

    String getMantaURL();
    String getMantaUser();
    String getMantaKeyId();
    String getMantaKeyPath();
    int getTimeout();
}
