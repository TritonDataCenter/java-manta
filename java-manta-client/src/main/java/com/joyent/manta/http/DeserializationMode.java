package com.joyent.manta.http;

import java.util.Objects;

/**
 * Enum representing how we handle deserialization.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 1.0.0
 */
public enum DeserializationMode {
    /** Deserialize to JSON. */
    ENTITY,
    /** Don't deserialize - just return null. */
    VOID,
    /** Return back the HTTP response headers. */
    HEADER_MAP;

    /**
     * Derive the deserialization mode from a set of rules that map it to
     * a given class.
     *
     * @param className Class to look up
     * @return deserialization mode
     */
    static DeserializationMode valueForClassName(final String className) {
        Objects.requireNonNull(className, "Class name must be present");

        switch (className) {
            case "class java.lang.Void":
                return VOID;
            // the non-shaded class name
            case "java.util.Map<java.lang.String, org.apache.http.Header>":
            // support the maven shaded class name as well
            case "java.util.Map<java.lang.String, com.joyent.manta.org.apache.http.Header>":
                return HEADER_MAP;
            default:
                return ENTITY;
        }
    }
}
