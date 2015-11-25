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
    /**
     * Environment variable for looking up a Manta URL.
     */
    public static final String MANTA_URL_ENV_KEY = "MANTA_URL";

    /**
     * Environment variable for looking up a Manta account.
     */
    public static final String MANTA_ACCOUNT_ENV_KEY = "MANTA_USER";

    /**
     * Environment variable for looking up a RSA fingerprint.
     */
    public static final String MANTA_KEY_ID_ENV_KEY = "MANTA_KEY_ID";

    /**
     * Environment variable for looking up a RSA private key path.
     */
    public static final String MANTA_KEY_PATH_ENV_KEY = "MANTA_KEY_PATH";

    /**
     * Environment variable for looking up a Manta timeout.
     */
    public static final String MANTA_TIMEOUT_ENV_KEY = "MANTA_TIMEOUT";

    /**
     * Array of all environment variable names used.
     */
    public static final String[] ALL_PROPERTIES = {
            MANTA_URL_ENV_KEY, MANTA_ACCOUNT_ENV_KEY, MANTA_KEY_ID_ENV_KEY,
            MANTA_KEY_PATH_ENV_KEY, MANTA_TIMEOUT_ENV_KEY
    };

    /**
     * Creates a new instance that provides configuration beans via the
     * values specified in environment variables.
     */
    public EnvVarConfigContext() {
    }

    /**
     * Get the value of an environment variable where an empty string is
     * converted to null.
     *
     * @param var Environment variable name
     * @return value of environment variable
     */
    private static String getEnv(final String var) {
        return MantaUtils.toStringEmptyToNull(System.getenv(var));
    }

    @Override
    public String getMantaURL() {
        return getEnv(MANTA_URL_ENV_KEY);
    }

    @Override
    public String getMantaUser() {
        return getEnv(MANTA_ACCOUNT_ENV_KEY);
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
    public String getMantaHomeDirectory() {
        return ConfigContext.deriveHomeDirectoryFromUser(getMantaUser());
    }

    @Override
    public Integer getTimeout() {
        String timeoutString = getEnv(MANTA_TIMEOUT_ENV_KEY);
        return MantaUtils.parseIntegerOrNull(timeoutString);
    }
}
