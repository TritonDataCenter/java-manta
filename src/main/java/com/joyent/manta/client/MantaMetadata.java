/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import org.apache.commons.collections4.Predicate;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.commons.collections4.map.PredicatedMap;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * <p></p>Class for storing Manta metadata information. All metadata keys must start
 * with the string "m-". Case insensitive {@link Map} implementation that checks
 * for valid metadata key names.</p>
 *
 * <p><em>Note:</em> In order to specify multiple headers (as a list) you will
 * need to use commas to delineate the values.</p>
 *
 * @author Elijah Zupancic
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
@NotThreadSafe
public class MantaMetadata implements Map<String, String>, Cloneable, Serializable {
    private static final long serialVersionUID = -8455066896469193612L;

    static char[] ILLEGAL_KEY_CHARS = "()<>@,;:</[]?={}\\ \n\t\r".toCharArray();

    private final PredicatedMap<String, String> innerMap;

    public MantaMetadata(Map<? extends String, ? extends String> m) {
        this();
        innerMap.putAll(m);
    }

    public MantaMetadata() {
        final Map<String, String> map = new CaseInsensitiveMap<>();
        final Predicate<String> keyPredicate = new HttpHeaderNameKeyPredicate();
        this.innerMap = PredicatedMap.predicatedMap(map, keyPredicate, null);
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return new MantaMetadata(this);
    }

    protected class HttpHeaderNameKeyPredicate implements Predicate<String> {
        @Override
        public boolean evaluate(String object) {
            return object != null &&
                   !object.isEmpty() &&
                   !hasIllegalChars(object) &&
                   isIso88591(object) &&
                   validPrefix(object);
        }

        private boolean isIso88591(final String input)
        {
            try {
                final byte[] bytes = input.getBytes("ISO-8859-1");
                final String result = new String(bytes, "ISO-8859-1");
                return result.equals(input);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("JVM doesn't support \"ISO-8859-1\" encoding", e);
            }
        }

        private boolean validPrefix(final String input) {
            return input.toLowerCase(Locale.ENGLISH).startsWith("m-");
        }

        private boolean hasIllegalChars(final String input) {
            final char[] chars = input.toCharArray();

            for (final char c : chars) {
                if (isControlCharacter(c)) {
                    return true;
                }

                for (char ILLEGAL_KEY_CHAR : ILLEGAL_KEY_CHARS) {
                    if (c == ILLEGAL_KEY_CHAR) {
                        return true;
                    }
                }
            }

            return false;
        }

        private boolean isControlCharacter(final char c) {
            final int intVal = (int)c;
            return intVal < 32;
        }
    }


    /**
     * Deletes user-supplied metadata associated with a Manta object.
     * @param key key to delete
     */
    public void delete(final String key) {
        put(key, null);
    }

    @Override
    public String merge(String key, String value, BiFunction<? super String, ? super String, ? extends String> remappingFunction) {
        return innerMap.merge(key, value, remappingFunction);
    }

    @Override
    public String compute(String key, BiFunction<? super String, ? super String, ? extends String> remappingFunction) {
        return innerMap.compute(key, remappingFunction);
    }

    @Override
    public String computeIfPresent(String key, BiFunction<? super String, ? super String, ? extends String> remappingFunction) {
        return innerMap.computeIfPresent(key, remappingFunction);
    }

    @Override
    public String computeIfAbsent(String key, Function<? super String, ? extends String> mappingFunction) {
        return innerMap.computeIfAbsent(key, mappingFunction);
    }

    @Override
    public String replace(String key, String value) {
        return innerMap.replace(key, value);
    }

    @Override
    public boolean replace(String key, String oldValue, String newValue) {
        return innerMap.replace(key, oldValue, newValue);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return innerMap.remove(key, value);
    }

    @Override
    public String putIfAbsent(String key, String value) {
        return innerMap.putIfAbsent(key, value);
    }

    @Override
    public void replaceAll(BiFunction<? super String, ? super String, ? extends String> function) {
        innerMap.replaceAll(function);
    }

    @Override
    public void forEach(BiConsumer<? super String, ? super String> action) {
        innerMap.forEach(action);
    }

    @Override
    public String getOrDefault(Object key, String defaultValue) {
        return innerMap.getOrDefault(key, defaultValue);
    }

    @Override
    public String toString() {
        return innerMap.toString();
    }

    @Override
    public int hashCode() {
        return innerMap.hashCode();
    }

    @Override
    public boolean equals(Object object) {
        return innerMap.equals(object);
    }

    @Override
    public Collection<String> values() {
        return innerMap.values();
    }

    @Override
    public int size() {
        return innerMap.size();
    }

    @Override
    public String remove(Object key) {
        return innerMap.remove(key);
    }

    @Override
    public Set<String> keySet() {
        return innerMap.keySet();
    }

    @Override
    public boolean isEmpty() {
        return innerMap.isEmpty();
    }

    @Override
    public String get(Object key) {
        return innerMap.get(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return innerMap.containsValue(value);
    }

    @Override
    public boolean containsKey(Object key) {
        return innerMap.containsKey(key);
    }

    @Override
    public void clear() {
        innerMap.clear();
    }

    @Override
    public String put(String key, String value) {
        return innerMap.put(key, value);
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> mapToCopy) {
        innerMap.putAll(mapToCopy);
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        return innerMap.entrySet();
    }
}
