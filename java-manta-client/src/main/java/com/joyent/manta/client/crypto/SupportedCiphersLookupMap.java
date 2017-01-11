package com.joyent.manta.client.crypto;

import com.joyent.manta.util.MantaUtils;
import org.apache.commons.collections4.map.CaseInsensitiveMap;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Custom built {@link Map} implementation that supports case-sensitive and
 * case-insensitive operations.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public final class SupportedCiphersLookupMap implements Map<String, SupportedCipherDetails> {
    /**
     * Wrapped unmodifiable map instance providing data.
     */
    private final Map<String, SupportedCipherDetails> wrapped;

    /**
     * Wrapped unmodifiable case-insensitive instance providing data.
     */
    private final CaseInsensitiveMap<String, SupportedCipherDetails> lowercaseWrapped;

    /**
     * Package default constructor because interface is through {@link SupportedCipherDetails}.
     */
    SupportedCiphersLookupMap() {
        this.wrapped = MantaUtils.unmodifiableMap(
                AesGcmCipherDetails.INSTANCE.getCipherAlgorithm(), AesGcmCipherDetails.INSTANCE,
                AesCtrCipherDetails.INSTANCE.getCipherAlgorithm(), AesCtrCipherDetails.INSTANCE,
                AesCbcCipherDetails.INSTANCE.getCipherAlgorithm(), AesCbcCipherDetails.INSTANCE
        );

        this.lowercaseWrapped = new CaseInsensitiveMap<>(this.wrapped);
    }

    /**
     * @return the key set all in lowercase.
     */
    public Set<String> lowercaseKeySet() {
        return this.lowercaseWrapped.keySet();
    }

    /**
     * Searches the map and determines if a case-insensitive key exists.
     * @param key case-insensitive key
     * @return true if exists otherwise false
     */
    public boolean containsKeyCaseInsensitive(final String key) {
        return this.lowercaseWrapped.containsKey(key);
    }

    /**
     * Searches the map for a value by a case-insensitive key.
     * @param key case-insensitive key
     * @return associated value or null when not found
     */
    public SupportedCipherDetails getWithCaseInsensitiveKey(final String key) {
        return this.lowercaseWrapped.get(key);
    }

    /* WRAPPED METHODS */

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
    public SupportedCipherDetails get(final Object key) {
        return wrapped.get(key);
    }

    @Override
    public SupportedCipherDetails put(final String key, final SupportedCipherDetails value) {
        return wrapped.put(key, value);
    }

    @Override
    public SupportedCipherDetails remove(final Object key) {
        return wrapped.remove(key);
    }

    @Override
    public void putAll(final Map<? extends String, ? extends SupportedCipherDetails> m) {
        wrapped.putAll(m);
    }

    @Override
    public void clear() {
        wrapped.clear();
    }

    @Override
    public Set<String> keySet() {
        return wrapped.keySet();
    }

    @Override
    public Collection<SupportedCipherDetails> values() {
        return wrapped.values();
    }

    @Override
    public Set<Entry<String, SupportedCipherDetails>> entrySet() {
        return wrapped.entrySet();
    }

    @Override
    public boolean equals(final Object o) {
        return wrapped.equals(o);
    }

    @Override
    public int hashCode() {
        return wrapped.hashCode();
    }

    @Override
    public SupportedCipherDetails getOrDefault(final Object key, final SupportedCipherDetails defaultValue) {
        return wrapped.getOrDefault(key, defaultValue);
    }

    @Override
    public void forEach(final BiConsumer<? super String, ? super SupportedCipherDetails> action) {
        wrapped.forEach(action);
    }

    @Override
    public void replaceAll(final BiFunction<? super String, ? super SupportedCipherDetails, ? extends SupportedCipherDetails> function) {
        wrapped.replaceAll(function);
    }

    @Override
    public SupportedCipherDetails putIfAbsent(final String key, final SupportedCipherDetails value) {
        return wrapped.putIfAbsent(key, value);
    }

    @Override
    public boolean remove(final Object key, final Object value) {
        return wrapped.remove(key, value);
    }

    @Override
    public boolean replace(final String key,
                           final SupportedCipherDetails oldValue, final SupportedCipherDetails newValue) {
        return wrapped.replace(key, oldValue, newValue);
    }

    @Override
    public SupportedCipherDetails replace(final String key, final SupportedCipherDetails value) {
        return wrapped.replace(key, value);
    }

    @Override
    public SupportedCipherDetails computeIfAbsent(final String key,
                                                  final Function<? super String, ? extends SupportedCipherDetails> mappingFunction) {
        return wrapped.computeIfAbsent(key, mappingFunction);
    }

    @Override
    public SupportedCipherDetails computeIfPresent(final String key,
                                                   final BiFunction<? super String, ? super SupportedCipherDetails, ? extends SupportedCipherDetails> remappingFunction) {
        return wrapped.computeIfPresent(key, remappingFunction);
    }

    @Override
    public SupportedCipherDetails compute(final String key,
                                          final BiFunction<? super String, ? super SupportedCipherDetails, ? extends SupportedCipherDetails> remappingFunction) {
        return wrapped.compute(key, remappingFunction);
    }

    @Override
    public SupportedCipherDetails merge(final String key, final SupportedCipherDetails value,
                                        final BiFunction<? super SupportedCipherDetails, ? super SupportedCipherDetails, ? extends SupportedCipherDetails> remappingFunction) {
        return wrapped.merge(key, value, remappingFunction);
    }
}
