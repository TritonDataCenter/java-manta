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
    /**
     * Manta service endpoint.
     */
    private String mantaURL;

    /**
     * Account associated with the Manta service.
     */
    private String account;

    /**
     * RSA key fingerprint of the private key used to access Manta.
     */
    private String mantaKeyId;

    /**
     * Path on the filesystem to the private RSA key used to access Manta.
     */
    private String mantaKeyPath;

    /**
     * General connection timeout for the Manta service.
     */
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
        return this.account;
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
    public String getMantaHomeDirectory() {
        return ConfigContext.deriveHomeDirectoryFromUser(getMantaUser());
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
            this.account = context.getMantaUser();
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

    /**
     * Sets the Manta service endpoint.
     * @param mantaURL Manta service endpoint
     * @return the current instance of {@link BaseChainedConfigContext}
     */
    public BaseChainedConfigContext setMantaURL(final String mantaURL) {
        this.mantaURL = mantaURL;
        return this;
    }

    /**
     * Sets the account associated with the Manta service.
     * @param mantaUser Manta user account
     * @return the current instance of {@link BaseChainedConfigContext}
     */
    public BaseChainedConfigContext setMantaUser(final String mantaUser) {
        this.account = mantaUser;
        return this;
    }

    /**
     * Sets the RSA key fingerprint of the private key used to access Manta.
     * @param mantaKeyId RSA key fingerprint
     * @return the current instance of {@link BaseChainedConfigContext}
     */
    public BaseChainedConfigContext setMantaKeyId(final String mantaKeyId) {
        this.mantaKeyId = mantaKeyId;
        return this;
    }

    /**
     * Sets the path on the filesystem to the private RSA key used to access Manta.
     * @param mantaKeyPath path on the filesystem
     * @return the current instance of {@link BaseChainedConfigContext}
     */
    public BaseChainedConfigContext setMantaKeyPath(final String mantaKeyPath) {
        this.mantaKeyPath = mantaKeyPath;
        return this;
    }

    /**
     * Sets the general connection timeout for the Manta service.
     * @param timeout timeout in milliseconds
     * @return the current instance of {@link BaseChainedConfigContext}
     */
    public BaseChainedConfigContext setTimeout(final Integer timeout) {
        this.timeout = timeout;
        return this;
    }
}
