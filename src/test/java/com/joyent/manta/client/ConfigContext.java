package com.joyent.manta.client;

/**
 * Interface representing the configuration properties needed to configure a
 * {@link MantaClient}.
 */
public interface ConfigContext {
    String mantaURL();
    String mantaUser();
    String mantaKeyId();
    String mantaKeyPath();
}
