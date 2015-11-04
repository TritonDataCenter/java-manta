/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.config;

/**
 * Abstract implementation of {@link ConfigContext} that allows for chaining
 * in default implementations of configuration that are delegate to when
 * we aren't passed a value.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public abstract class BaseChainedConfigContext implements ConfigContext {
    private String mantaURL;
    private String mantaUser;
    private String mantaKeyId;
    private String mantaKeyPath;
    private Integer timeout;

    /** Singleton instance of default configuration for easy reference. */
    public static final ConfigContext DEFAULT_CONFIG =
            new DefaultsConfigContext();

    /**
     * Constructor that prepopulates configuration context with the default
     * values.
     */
    public BaseChainedConfigContext() {
    }

    /**
     * Constructor that takes a default value for each one of the configuration
     * values.
     *
     * @param defaultingContext context that provides default values
     */
    public BaseChainedConfigContext(final ConfigContext defaultingContext) {
        overwriteWithContext(defaultingContext);
    }

    @Override
    public String getMantaURL() {
        return this.mantaURL;
    }

    @Override
    public String getMantaUser() {
        return this.mantaUser;
    }

    @Override
    public String getMantaKeyId() {
        return this.mantaKeyId;
    }

    @Override
    public String getMantaKeyPath() {
        return this.mantaKeyPath;
    }

    @Override
    public Integer getTimeout() {
        return this.timeout;
    }

    /**
     * Overwrites the configuration values with the values of the passed context
     * if those values are not null and aren't empty.
     *
     * @param context context to overwrite configuration with
     */
    public void overwriteWithContext(final ConfigContext context) {
        if (isPresent(context.getMantaURL())) {
            this.mantaURL = context.getMantaURL();
        }

        if (isPresent(context.getMantaUser())) {
            this.mantaUser = context.getMantaUser();
        }

        if (isPresent(context.getMantaKeyId())) {
            this.mantaKeyId = context.getMantaKeyId();
        }

        if (isPresent(context.getMantaKeyPath())) {
            this.mantaKeyPath = context.getMantaKeyPath();
        }

        if (context.getTimeout() != null) {
            this.timeout = context.getTimeout();
        }
    }

    /**
     * Checks to see that a given string is neither empty nor null.
     * @param string string to check
     * @return true when string is non-null and not empty
     */
    protected static boolean isPresent(final String string) {
        return string != null && !string.isEmpty();
    }

    public BaseChainedConfigContext setMantaURL(final String mantaURL) {
        this.mantaURL = mantaURL;
        return this;
    }

    public BaseChainedConfigContext setMantaUser(final String mantaUser) {
        this.mantaUser = mantaUser;
        return this;
    }

    public BaseChainedConfigContext setMantaKeyId(final String mantaKeyId) {
        this.mantaKeyId = mantaKeyId;
        return this;
    }

    public BaseChainedConfigContext setMantaKeyPath(final String mantaKeyPath) {
        this.mantaKeyPath = mantaKeyPath;
        return this;
    }

    public BaseChainedConfigContext setTimeout(final Integer timeout) {
        this.timeout = timeout;
        return this;
    }
}
