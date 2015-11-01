/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.config;

import com.joyent.manta.client.MantaUtils;

/**
 * An implementation of {@link ConfigContext} that reads its configuration
 * from expected environment variables.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class EnvVarConfigContext implements ConfigContext {
    public static final String MANTA_URL_ENV_KEY = "MANTA_URL";
    public static final String MANTA_USER_ENV_KEY = "MANTA_USER";
    public static final String MANTA_KEY_ID_ENV_KEY = "MANTA_KEY_ID";
    public static final String MANTA_KEY_PATH_ENV_KEY = "MANTA_KEY_PATH";
    public static final String MANTA_TIMEOUT_ENV_KEY = "MANTA_TIMEOUT";

    public static final String[] ALL_PROPERTIES = {
            MANTA_URL_ENV_KEY, MANTA_USER_ENV_KEY, MANTA_KEY_ID_ENV_KEY,
            MANTA_KEY_PATH_ENV_KEY, MANTA_TIMEOUT_ENV_KEY
    };

    public EnvVarConfigContext() {
    }

    private static String getEnv(String var) {
        return MantaUtils.toStringEmptyToNull(System.getenv(var));
    }

    @Override
    public String getMantaURL() {
        return getEnv(MANTA_URL_ENV_KEY);
    }

    @Override
    public String getMantaUser() {
        return getEnv(MANTA_USER_ENV_KEY);
    }

    @Override
    public String getMantaKeyId() {
        return getEnv(MANTA_KEY_ID_ENV_KEY);
    }

    @Override
    public String getMantaKeyPath() {
        return getEnv(MANTA_KEY_PATH_ENV_KEY);
    }

    @Override
    public Integer getTimeout() {
        String timeoutString = getEnv(MANTA_TIMEOUT_ENV_KEY);
        return MantaUtils.parseIntegerOrNull(timeoutString);
    }
}
