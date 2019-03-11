/*
 * Copyright (c) 2019, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.commons.collections4.map.UnmodifiableMap;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Custom built {@link Map} implementation that supports case-sensitive and
 * case-insensitive operations.
 *
 * @param <K> key as string because we provide case-insensitive operations
 * @param <V> value to be looking up
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @author <a href="https://github.com/nairashwin952013">Ashwin A Nair</a>
 * @since 3.0.0
 */
public class LookupMap<K extends String, V> implements Map<K, V>  {
    /**
     /**
     * Wrapped unmodifiable map instance providing data.
     */
    private final Map<K, V> wrapped;

    /**
     * Wrapped unmodifiable case-insensitive instance providing data.
     */
    private final CaseInsensitiveMap<K, V> lowercaseWrapped;

    /**
     * Class name for unmodifiable maps in Java Utils.
     */
    private static final String JAVA_UNMODIFIABLE_MAP = "java.util.Collections$UnmodifiableMap";

    /**
     * Class name for unmodifiable maps in Apache Collections.
     */
    private static final String APACHE_UNMODIFIABLE_MAP = "org.apache.commons.collections4.map.UnmodifiableMap";

    /**
     * Class name for immutable maps in Google Collections.
     */
    private static final String IMMUTABLE_MAP = "com.google.common.collect.ImmutableMap";

    /**
     * Creates a new instance of a lookup map backed by the specified map.
     *
     * @param backingMap map to back lookup map
     */
    public LookupMap(final Map<K, V> backingMap) {
        if (isUnmodifiable(backingMap)) {
            this.wrapped = backingMap;
        } else {
            this.wrapped = Collections.unmodifiableMap(backingMap);
        }
        this.lowercaseWrapped = new CaseInsensitiveMap<>(this.wrapped);
    }

    /**
     * Determines if a given map is of a unmodifiable type.
     *
     * @param map map to check
     * @return true if unmodifiable
     */
    private boolean isUnmodifiable(final Map<K, V> map) {
        return map.getClass().getName().equals(JAVA_UNMODIFIABLE_MAP)
               || map.getClass().getName().equals(APACHE_UNMODIFIABLE_MAP)
               || map.getClass().getName().equals(IMMUTABLE_MAP)
               || map instanceof UnmodifiableMap;
    }

    /**
     * @return the key set all in lowercase.
     */
    public Set<K> lowercaseKeySet() {
        return this.lowercaseWrapped.keySet();
    }

    /**
     * Searches the map and determines if a case-insensitive key exists.
     * @param key case-insensitive key
     * @return true if exists otherwise false
     */
    public boolean containsKeyCaseInsensitive(final K key) {
        return this.lowercaseWrapped.containsKey(key);
    }

    /**
     * Searches the map for a value by a case-insensitive key.
     * @param key case-insensitive key
     * @return associated value or null when not found
     */
    public V getWithCaseInsensitiveKey(final K key) {
        return this.lowercaseWrapped.get(key);
    }

    @Override
    public int size() {
        return wrapped.size();
    }

    @Override
    public boolean isEmpty() {
        return wrapped.isEmpty();
    }

    @Override
    public boolean containsKey(final Object key) {
        return wrapped.containsKey(key);
    }

    @Override
    public boolean containsValue(final Object value) {
        return wrapped.containsValue(value);
    }

    @Override
    public V get(final Object key) {
        return wrapped.get(key);
    }

    @Override
    public V put(final K key, final V value) {
        return wrapped.put(key, value);
    }

    @Override
    public V remove(final Object key) {
        return wrapped.remove(key);
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> m) {
        wrapped.putAll(m);
    }

    @Override
    public void clear() {
        wrapped.clear();
    }

    @Override
    public Set<K> keySet() {
        return wrapped.keySet();
    }

    @Override
    public Collection<V> values() {
        return wrapped.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return wrapped.entrySet();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Map)) {
            return false;
        }

        final Map that = (Map) o;
        return Objects.equals(wrapped, that);
    }

    @Override
    public int hashCode() {
        return wrapped.hashCode();
    }

    @Override
    public V getOrDefault(final Object key, final V defaultValue) {
        return wrapped.getOrDefault(key, defaultValue);
    }

    @Override
    public void forEach(final BiConsumer<? super K, ? super V> action) {
        wrapped.forEach(action);
    }

    @Override
    public void replaceAll(final BiFunction<? super K, ? super V, ? extends V> function) {
        wrapped.replaceAll(function);
    }

    @Override
    public V putIfAbsent(final K key, final V value) {
        return wrapped.putIfAbsent(key, value);
    }

    @Override
    public boolean remove(final Object key, final Object value) {
        return wrapped.remove(key, value);
    }

    @Override
    public boolean replace(final K key, final V oldValue, final V newValue) {
        return wrapped.replace(key, oldValue, newValue);
    }

    @Override
    public V replace(final K key, final V value) {
        return wrapped.replace(key, value);
    }

    @Override
    public V computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction) {
        return wrapped.computeIfAbsent(key, mappingFunction);
    }

    @Override
    public V computeIfPresent(final K key,
                              final BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        return wrapped.computeIfPresent(key, remappingFunction);
    }

    @Override
    public V compute(final K key,
                     final BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        return wrapped.compute(key, remappingFunction);
    }

    @Override
    public V merge(final K key,
                   final V value,
                   final BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        return wrapped.merge(key, value, remappingFunction);
    }
}
