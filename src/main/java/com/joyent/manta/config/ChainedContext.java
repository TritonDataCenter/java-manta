/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.config;

/**
 * Implementation of {@link ConfigContext} that links together multiple contexts.
 * This allows you to create tiers of configuration in which certain configuration
 * contexts are given priority over others.
 */
public class ChainedContext extends StandardConfigContext {
    public ChainedContext(ConfigContext... contexts) {
        for (ConfigContext c : contexts) {
            if (isPresent(c.getMantaKeyId())) setMantaKeyId(c.getMantaKeyId());
            if (isPresent(c.getMantaKeyPath())) setMantaKeyPath(c.getMantaKeyPath());
            if (isPresent(c.getMantaURL())) setMantaURL(c.getMantaURL());
            if (isPresent(c.getMantaUser())) setMantaUser(c.getMantaUser());
            setTimeout(c.getTimeout());
        }
    }

    /**
     * Checks to see that a given string is neither empty nor null.
     * @param string string to check
     * @return true when string is non-null and not empty
     */
    protected static boolean isPresent(String string) {
        return string != null && !string.isEmpty();
    }
}
