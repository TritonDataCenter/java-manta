package com.joyent.manta.config;

import com.joyent.manta.client.MantaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

import static com.joyent.manta.config.EnvVarConfigContext.*;

/**
 * {@link ConfigContext} implementation that is used for configuring instances
 * from a Map.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MapConfigContext implements ConfigContext {
    public static final String MANTA_URL_KEY = "manta.url";
    public static final String MANTA_USER_KEY = "manta.user";
    public static final String MANTA_KEY_ID_KEY = "manta.key_id";
    public static final String MANTA_KEY_PATH_KEY = "manta.key_path";
    public static final String MANTA_TIMEOUT_KEY = "manta.timeout";
    
    // I know manually adding them all sucks, but it is the simplest operation
    // for a shared library. We could do all sorts of complicated reflection
    // or annotation processing, but they are error-prone.
    /** List of all properties that we read from configuration. */
    public static final String[] ALL_PROPERTIES = {
            MANTA_URL_KEY, MANTA_USER_KEY, MANTA_KEY_ID_KEY,
            MANTA_KEY_PATH_KEY, MANTA_TIMEOUT_KEY
    };

    private final Map<?, ?> backingMap;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public MapConfigContext(Map<?, ?> backingMap) {
        this.backingMap = backingMap;
    }

    @Override
    public String getMantaURL() {
        return normalizeEmptyAndNullAndDefaultToStringValue(MANTA_URL_KEY, MANTA_URL_ENV_KEY);
    }

    @Override
    public String getMantaUser() {
        return normalizeEmptyAndNullAndDefaultToStringValue(MANTA_USER_KEY, MANTA_USER_ENV_KEY);
    }

    @Override
    public String getMantaKeyId() {
        return normalizeEmptyAndNullAndDefaultToStringValue(MANTA_KEY_ID_KEY, MANTA_KEY_ID_ENV_KEY);
    }

    @Override
    public String getMantaKeyPath() {
        return normalizeEmptyAndNullAndDefaultToStringValue(MANTA_KEY_PATH_KEY, MANTA_KEY_PATH_ENV_KEY);
    }

    @Override
    public Integer getTimeout() {
        Integer mapValue = MantaUtils.parseIntegerOrNull(backingMap.get(MANTA_TIMEOUT_KEY));

        if (mapValue != null) return mapValue;

        return MantaUtils.parseIntegerOrNull(backingMap.get(MANTA_TIMEOUT_ENV_KEY));
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
    Object put(String key, String value) {
        if (key == null) throw new IllegalArgumentException("Config key can't be null");
        if (key.isEmpty()) throw new IllegalArgumentException("Config key can't be blank");

        // Java generics can be stupid
        @SuppressWarnings("unchecked")
        Map<Object, Object> map = (Map<Object, Object>)this.backingMap;
        return map.put(key, value);
    }

    private String normalizeEmptyAndNullAndDefaultToStringValue(Object... keys) {
        for (Object k : keys) {
            String value = MantaUtils.toStringEmptyToNull(backingMap.get(k));
            if (value == null) continue;

            return value;
        }

        // We couldn't find any values
        return null;
    }
}
