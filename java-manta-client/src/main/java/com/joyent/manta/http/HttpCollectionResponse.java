package com.joyent.manta.http;

import org.apache.http.HttpResponse;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;

/**
 * This is a delegate or wrapper class that wraps an instance of
 * {@link Collection}, so that the consumer can get additional data about
 * the {@link HttpResponse} in which the collection was derived from.
 *
 * @param <E> the type of elements in this collection
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class HttpCollectionResponse<E> implements Collection<E> {
    /**
     * Wrapped inner collection object.
     */
    private final Collection<E> wrapped;

    /**
     * Embedded HTTP response object.
     */
    private final HttpResponse response;

    /**
     * Creates a new instance of a wrapped {@link Collection}.
     *
     * @param wrapped inner collection object to wrap
     * @param response HTTP response object to embed
     */
    public HttpCollectionResponse(final Collection<E> wrapped, final HttpResponse response) {
        Objects.requireNonNull(wrapped, "Wrapped collection must not be null");
        Objects.requireNonNull(response, "Embedded HTTP response must not be null");

        this.wrapped = wrapped;
        this.response = response;
    }

    public HttpResponse getResponse() {
        return response;
    }

    public Collection<E> getWrapped() {
        return wrapped;
    }

    @Override
    public int hashCode() {
        return wrapped.hashCode();
    }

    @Override
    public boolean equals(final Object o) {
        return wrapped.equals(o);
    }

    @Override
    public void clear() {
        wrapped.clear();
    }

    @Override
    public boolean retainAll(final Collection<?> c) {
        return wrapped.retainAll(c);
    }

    @Override
    public boolean removeAll(final Collection<?> c) {
        return wrapped.removeAll(c);
    }

    @Override
    public boolean addAll(final Collection<? extends E> c) {
        return wrapped.addAll(c);
    }

    @Override
    public boolean containsAll(final Collection<?> c) {
        return wrapped.containsAll(c);
    }

    @Override
    public boolean remove(final Object o) {
        return wrapped.remove(o);
    }

    @Override
    public boolean add(final E e) {
        return wrapped.add(e);
    }

    @Override
    public <T1> T1[] toArray(final T1[] a) {
        return wrapped.toArray(a);
    }

    @Override
    public Object[] toArray() {
        return wrapped.toArray();
    }

    @Override
    public Iterator<E> iterator() {
        return wrapped.iterator();
    }

    @Override
    public boolean contains(final Object o) {
        return wrapped.contains(o);
    }

    @Override
    public boolean isEmpty() {
        return wrapped.isEmpty();
    }

    @Override
    public int size() {
        return wrapped.size();
    }
}
