/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.config;

/**
 * Interface representing the configuration properties needed to configure a
 * {@link com.joyent.manta.client.MantaClient}.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public interface ConfigContext {
    /**
     * @return Manta service endpoint.
     */
    String getMantaURL();

    /**
     * @return User account associated with the Manta service.
     */
    String getMantaUser();

    /**
     * @return RSA key fingerprint of the private key used to access Manta.
     */
    String getMantaKeyId();

    /**
     * @return Path on the filesystem to the private RSA key used to access Manta.
     */
    String getMantaKeyPath();

    /**
     * @return General connection timeout for the Manta service.
     */
    Integer getTimeout();
}
