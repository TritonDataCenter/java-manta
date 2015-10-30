/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.config;

/**
 * An implementation of {@link ConfigContext} that reads its configuration
 * from expected environment variables.
 */
public class EnvVarConfigContext implements ConfigContext {
    public EnvVarConfigContext() {
    }

    private static String getEnv(String var) {
        return System.getenv(var);
    }

    @Override
    public String getMantaURL() {
        return getEnv("MANTA_URL");
    }

    @Override
    public String getMantaUser() {
        return getEnv("MANTA_USER");
    }

    @Override
    public String getMantaKeyId() {
        return getEnv("MANTA_KEY_ID");
    }

    @Override
    public String getMantaKeyPath() {
        return getEnv("MANTA_KEY_PATH");
    }

    @Override
    public int getTimeout() {
        // NOTE: We may want to think about offering this as an environment variable
        return DEFAULT_HTTP_TIMEOUT;
    }
}
