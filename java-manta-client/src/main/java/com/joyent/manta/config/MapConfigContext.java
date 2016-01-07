package com.joyent.manta.config;

import com.joyent.manta.client.MantaUtils;

import java.util.Map;

import static com.joyent.manta.config.EnvVarConfigContext.*;

/**
 * {@link ConfigContext} implementation that is used for configuring instances
 * from a Map.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MapConfigContext implements ConfigContext {
    /**
     * Property key for looking up a Manta URL.
     */
    public static final String MANTA_URL_KEY = "manta.url";

    /**
     * Property key for looking up a Manta account.
     */
    public static final String MANTA_USER_KEY = "manta.user";

    /**
     * Property key for looking up a RSA fingerprint.
     */
    public static final String MANTA_KEY_ID_KEY = "manta.key_id";

    /**
     * Property key for looking up a RSA private key path.
     */
    public static final String MANTA_KEY_PATH_KEY = "manta.key_path";

    /**
     * Property key for looking up a Manta timeout.
     */
    public static final String MANTA_TIMEOUT_KEY = "manta.timeout";

    /**
     * Property key for number of times to retry failed requests.
     */
    public static final String MANTA_RETRIES_KEY = "manta.retries";

    /**
     * Property key for the maximum number of open connections to the Manta API.
     */
    public static final String MANTA_MAX_CONNS_KEY = "manta.max_connections";

    /**
     * Property key for looking up Manta private key content.
     */
    public static final String MANTA_PRIVATE_KEY_CONTENT_KEY = "manta.key_content";

    /**
     * Property key for looking up Manta password.
     */
    public static final String MANTA_PASSWORD_KEY = "manta.password";

    /**
     * Property key for setting HttpTransport implementation.
     */
    public static final String MANTA_HTTP_TRANSPORT_KEY = "manta.http_transport";

    /**
     * Property key for setting TLS protocols.
     */
    public static final String MANTA_HTTPS_PROTOCOLS_KEY = "https.protocols";

    /**
     * Property key for setting TLS ciphers.
     */
    public static final String MANTA_HTTPS_CIPHERS_KEY = "https.cipherSuites";

    /**
     * Property key for disabling HTTP signatures.
     */
    public static final String MANTA_NO_AUTH_KEY = "manta.no_auth";

    /**
     * Property key for disabling native code support for generating signatures.
     */
    public static final String MANTA_NO_NATIVE_SIGS_KEY = "manta.disable_native_sigs";

    /**
     * Property key for looking up the time in milliseconds to cache HTTP signature headers.
     */
    public static final String MANTA_SIGS_CACHE_TTL_KEY = "http.signature.cache.ttl";

    // I know manually adding them all sucks, but it is the simplest operation
    // for a shared library. We could do all sorts of complicated reflection
    // or annotation processing, but they are error-prone.
    /**
     * List of all properties that we read from configuration.
     */
    public static final String[] ALL_PROPERTIES = {
            MANTA_URL_KEY, MANTA_USER_KEY, MANTA_KEY_ID_KEY,
            MANTA_KEY_PATH_KEY, MANTA_TIMEOUT_KEY, MANTA_RETRIES_KEY,
            MANTA_MAX_CONNS_KEY, MANTA_PRIVATE_KEY_CONTENT_KEY,
            MANTA_PASSWORD_KEY, MANTA_HTTP_TRANSPORT_KEY,
            MANTA_HTTPS_PROTOCOLS_ENV_KEY, MANTA_HTTPS_CIPHERS_KEY,
            MANTA_NO_AUTH_KEY, MANTA_NO_NATIVE_SIGS_KEY,
            MANTA_SIGS_CACHE_TTL_KEY
    };

    /**
     * Internal map used as the source of the configuration bean values.
     */
    private final Map<?, ?> backingMap;

    /**
     * Creates a new instance using the passed {@link Map} implementation as
     * a backing store.
     *
     * @param backingMap Map implementation used for the values of the configuration beans
     */
    public MapConfigContext(final Map<?, ?> backingMap) {
        this.backingMap = backingMap;
    }

    @Override
    public String getMantaURL() {
        return normalizeEmptyAndNullAndDefaultToStringValue(
                MANTA_URL_KEY, MANTA_URL_ENV_KEY);
    }

    @Override
    public String getMantaUser() {
        return normalizeEmptyAndNullAndDefaultToStringValue(
                MANTA_USER_KEY, MANTA_ACCOUNT_ENV_KEY);
    }

    @Override
    public String getMantaKeyId() {
        return normalizeEmptyAndNullAndDefaultToStringValue(
                MANTA_KEY_ID_KEY, MANTA_KEY_ID_ENV_KEY);
    }

    @Override
    public String getMantaKeyPath() {
        return normalizeEmptyAndNullAndDefaultToStringValue(
                MANTA_KEY_PATH_KEY, MANTA_KEY_PATH_ENV_KEY);
    }

    @Override
    public String getPrivateKeyContent() {
        return normalizeEmptyAndNullAndDefaultToStringValue(
                MANTA_PRIVATE_KEY_CONTENT_KEY, MANTA_PRIVATE_KEY_CONTENT_ENV_KEY);
    }

    @Override
    public String getPassword() {
        return normalizeEmptyAndNullAndDefaultToStringValue(
                MANTA_PASSWORD_KEY, MANTA_PASSWORD_ENV_KEY);
    }

    @Override
    public String getMantaHomeDirectory() {
        return ConfigContext.deriveHomeDirectoryFromUser(getMantaUser());
    }

    @Override
    public Integer getTimeout() {
        Integer mapValue = MantaUtils.parseIntegerOrNull(backingMap.get(MANTA_TIMEOUT_KEY));

        if (mapValue != null) {
            return mapValue;
        }

        return MantaUtils.parseIntegerOrNull(backingMap.get(MANTA_TIMEOUT_ENV_KEY));
    }

    @Override
    public Integer getRetries() {
        Integer mapValue = MantaUtils.parseIntegerOrNull(backingMap.get(MANTA_RETRIES_KEY));

        if (mapValue != null) {
            return mapValue;
        }

        return MantaUtils.parseIntegerOrNull(backingMap.get(MANTA_RETRIES_ENV_KEY));
    }

    @Override
    public Integer getMaximumConnections() {
        Integer mapValue = MantaUtils.parseIntegerOrNull(backingMap.get(MANTA_MAX_CONNS_KEY));

        if (mapValue != null) {
            return mapValue;
        }

        return MantaUtils.parseIntegerOrNull(backingMap.get(MANTA_MAX_CONNS_ENV_KEY));
    }

    @Override
    public String getHttpTransport() {
        return normalizeEmptyAndNullAndDefaultToStringValue(
                MANTA_HTTP_TRANSPORT_KEY, MANTA_HTTP_TRANSPORT_ENV_KEY);
    }

    @Override
    public String getHttpsProtocols() {
        return normalizeEmptyAndNullAndDefaultToStringValue(
                MANTA_HTTPS_PROTOCOLS_KEY, MANTA_HTTPS_PROTOCOLS_ENV_KEY);
    }

    @Override
    public String getHttpsCipherSuites() {
        return normalizeEmptyAndNullAndDefaultToStringValue(
                MANTA_HTTPS_CIPHERS_KEY, MANTA_HTTPS_CIPHERS_ENV_KEY);
    }

    @Override
    public Boolean noAuth() {
        Boolean mapValue = MantaUtils.parseBooleanOrNull(backingMap.get(MANTA_NO_AUTH_KEY));

        if (mapValue != null) {
            return mapValue;
        }

        return MantaUtils.parseBooleanOrNull(backingMap.get(MANTA_NO_AUTH_ENV_KEY));
    }

    @Override
    public Boolean disableNativeSignatures() {
        Boolean mapValue = MantaUtils.parseBooleanOrNull(backingMap.get(MANTA_NO_NATIVE_SIGS_KEY));

        if (mapValue != null) {
            return mapValue;
        }

        return MantaUtils.parseBooleanOrNull(backingMap.get(MANTA_NO_NATIVE_SIGS_ENV_KEY));
    }

    @Override
    public Integer getSignatureCacheTTL() {
        Integer mapValue = MantaUtils.parseIntegerOrNull(backingMap.get(MANTA_SIGS_CACHE_TTL_KEY));

        if (mapValue != null) {
            return mapValue;
        }

        return MantaUtils.parseIntegerOrNull(backingMap.get(MANTA_SIGS_CACHE_TTL_ENV_KEY));
    }

    /**
     * Allows the caller to perform a put operation on the backing map of the
     * context. This is typically used by other {@link ConfigContext}
     * implementations that need to cobble together multiple map values.
     *
     * This method is scoped to default because no other packages should be
     * using it.
     *
     * @param key configuration key
     * @param value configuration value
     * @return return value of the put() operation from the backing map
     */
    Object put(final String key, final String value) {
        if (key == null) {
            throw new IllegalArgumentException("Config key can't be null");
        }

        if (key.isEmpty()) {
            throw new IllegalArgumentException("Config key can't be blank");
        }

        // Java generics can be stupid
        @SuppressWarnings("unchecked")
        Map<Object, Object> map = (Map<Object, Object>)this.backingMap;
        return map.put(key, value);
    }

    /**
     * Normalizes a value pulled from the backingMap.
     * @param keys key to use to pull value from backing map
     * @return null on empty string or null, otherwise value from backing map
     */
    private String normalizeEmptyAndNullAndDefaultToStringValue(final Object... keys) {
        for (Object k : keys) {
            String value = MantaUtils.toStringEmptyToNull(backingMap.get(k));
            if (value == null) {
                continue;
            }

            return value;
        }

        // We couldn't find any values
        return null;
    }

    @Override
    public String toString() {
        return ConfigContext.toString(this);
    }
}
