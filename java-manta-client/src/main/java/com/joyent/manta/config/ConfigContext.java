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
     * @return account associated with the Manta service.
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
     * @return private key content. This can't be set if the MantaKeyPath is set.
     */
    String getPrivateKeyContent();

    /**
     * @return password for private key. This is optional and typically not set.
     */
    String getPassword();

    /**
     * @return General connection timeout for the Manta service.
     */
    Integer getTimeout();

    /**
     * @return String of home directory based on Manta username.
     */
    String getMantaHomeDirectory();

    /**
     * @return Number of HTTP retries to perform on failure.
     */
    Integer getRetries();

    /**
     * @return the maximum number of open connections to the Manta API
     */
    Integer getMaximumConnections();

    /**
     * @return the class name of the {@link com.google.api.client.http.HttpTransport} implementation to use
     */
    String getHttpTransport();

    /**
     * @return a comma delimited list of HTTPS protocols
     */
    String getHttpsProtocols();

    /**
     * @return a comma delimited list of HTTPS cipher suites in order of preference
     */
    String getHttpsCipherSuites();

    /**
     * @return true when we disable sending HTTP signatures
     */
    Boolean noAuth();

    /**
     * @return true when we disable using native code to generate HTTP signatures
     */
    Boolean disableNativeSignatures();

    /**
     * @return time in milliseconds to cache HTTP signature headers
     */
    Integer getSignatureCacheTTL();

    /**
     * @return true when client-side encryption is enabled.
     */
    Boolean isClientEncryptionEnabled();

    /**
     * @return true when downloading unencrypted files is allowed in encryption mode
     */
    Boolean permitUnencryptedDownloads();

    /**
     * @return specifies if we are in strict ciphertext authentication mode or not
     */
    EncryptionObjectAuthenticationMode getEncryptionAuthenticationMode();

    /**
     * @return path to the private encryption key on the filesystem (can't be used if private key bytes is not null)
     */
    String getEncryptionPrivateKeyPath();

    /**
     * @return private encryption key data (can't be used if private key path is not null)
     */
    byte[] getEncryptionPrivateKeyBytes();

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

    /**
     * Utility method for generating to string values for all {@link ConfigContext}
     * implementations.
     *
     * @param context Context to generate String value from
     * @return string value of context
     */
    static String toString(final ConfigContext context) {
        final StringBuilder sb = new StringBuilder("BaseChainedConfigContext{");
        sb.append("mantaURL='").append(context.getMantaURL()).append('\'');
        sb.append(", user='").append(context.getMantaUser()).append('\'');
        sb.append(", mantaKeyId='").append(context.getMantaKeyId()).append('\'');
        sb.append(", mantaKeyPath='").append(context.getMantaKeyPath()).append('\'');
        sb.append(", timeout=").append(context.getTimeout());
        sb.append(", retries=").append(context.getRetries());
        sb.append(", maxConnections=").append(context.getMaximumConnections());
        sb.append(", httpTransport='").append(context.getHttpTransport()).append('\'');
        sb.append(", httpsProtocols='").append(context.getHttpsProtocols()).append('\'');
        sb.append(", httpsCiphers='").append(context.getHttpsCipherSuites()).append('\'');
        sb.append(", noAuth=").append(context.noAuth());
        sb.append(", disableNativeSignatures=").append(context.disableNativeSignatures());
        sb.append(", signatureCacheTTL=").append(context.getSignatureCacheTTL());
        sb.append(", clientEncryptionEnabled=").append(context.isClientEncryptionEnabled());
        sb.append(", permitUnencryptedDownloads=").append(context.permitUnencryptedDownloads());
        sb.append(", encryptionAuthenticationMode=").append(context.getEncryptionAuthenticationMode());
        sb.append(", encryptionPrivateKeyPath=").append(context.getEncryptionPrivateKeyPath());

        if (context.getEncryptionPrivateKeyBytes() == null) {
            sb.append(", encryptionPrivateKeyBytesLength=").append("null object");
        } else {
            sb.append(", encryptionPrivateKeyBytesLength=").append(context.getEncryptionPrivateKeyBytes().length);
        }

        sb.append('}');
        return sb.toString();
    }
}
