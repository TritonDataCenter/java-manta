/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.config;

import com.joyent.manta.client.MantaUtils;

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

    /**
     * @return String of home directory based on Manta username.
     */
    String getMantaHomeDirectory();

    /**
     * Extracts the home directory based on the Manta account name.
     *
     * @param mantaUser user associated with account
     * @return the root account holder name prefixed with a slash or null
     *         upon null mantaUser
     */
    static String deriveHomeDirectoryFromUser(final String mantaUser) {
        if (mantaUser == null) {
            return null;
        }

        final String[] accountParts = MantaUtils.parseAccount(mantaUser);
        return String.format("/%s", accountParts[0]);
    }
}
