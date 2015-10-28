package com.joyent.manta.client;

/**
 * An implementation of {@link ConfigContext} that reads its configuration
 * from expected environment variables.
 */
public class EnvVarConfigContext implements ConfigContext {
    public EnvVarConfigContext() {
    }

    private static String getAndCheckEnvVar(String var) {
        String url = System.getenv(var);

        if (url == null || url.isEmpty()) {
            String msg = String.format("%s environment variable must be set", var);
            throw new IllegalArgumentException(msg);
        }

        return url;
    }

    @Override
    public String mantaURL() {
        return getAndCheckEnvVar("MANTA_URL");
    }

    @Override
    public String mantaUser() {
        return getAndCheckEnvVar("MANTA_USER");
    }

    @Override
    public String mantaKeyId() {
        return getAndCheckEnvVar("MANTA_KEY_ID");
    }

    @Override
    public String mantaKeyPath() {
        return getAndCheckEnvVar("MANTA_KEY_PATH");
    }
}
