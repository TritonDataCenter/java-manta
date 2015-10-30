package com.joyent.manta.config;

/**
 * Configuration context that is entirely driven by in-memory parameters.
 */
public class StandardConfigContext implements ConfigContext {
    private String mantaURL;
    private String mantaUser;
    private String mantaKeyId;
    private String mantaKeyPath;
    private int timeout = ConfigContext.DEFAULT_HTTP_TIMEOUT;

    public StandardConfigContext() {
    }

    public StandardConfigContext setMantaURL(String mantaURL) {
        this.mantaURL = mantaURL;
        return this;
    }

    public StandardConfigContext setMantaUser(String mantaUser) {
        this.mantaUser = mantaUser;
        return this;
    }

    public StandardConfigContext setMantaKeyId(String mantaKeyId) {
        this.mantaKeyId = mantaKeyId;
        return this;
    }

    public StandardConfigContext setMantaKeyPath(String mantaKeyPath) {
        this.mantaKeyPath = mantaKeyPath;
        return this;
    }

    public StandardConfigContext setTimeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    @Override
    public String getMantaURL() {
        return mantaURL;
    }

    @Override
    public String getMantaUser() {
        return mantaUser;
    }

    @Override
    public String getMantaKeyId() {
        return mantaKeyId;
    }

    @Override
    public String getMantaKeyPath() {
        return mantaKeyPath;
    }

    @Override
    public int getTimeout() {
        return timeout;
    }
}
