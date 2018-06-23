/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.http.HttpException;

import java.util.Objects;

import static org.apache.commons.lang3.Validate.notNull;

/**
 * A value object for <a href="https://tools.ietf.org/html/rfc7233#section-3">Range</a> and
 * <a href="https://tools.ietf.org/html/rfc7233#section-4.2">Content-Range</a> headers.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 * @since 3.2.2
 */
abstract class HttpRange {

    /**
     * The regex used to validateResponseWithMarker request Range header values.
     */
    private static final String REGEX_REQUEST_RANGE = "bytes=[0-9]+-[0-9]+";

    /**
     * The regex used to validateResponseWithMarker Content-Range header values.
     */
    private static final String REGEX_CONTENT_RANGE = "bytes [0-9]+-[0-9]+/[0-9]+";

    /**
     * The number of segments the Range regex expects to find.
     */
    private static final int PART_COUNT_RANGE = 2;

    /**
     * The number of segments the Content-Range regex expects to find.
     */
    private static final int PART_COUNT_CONTENT_RANGE = 3;

    /**
     * The start of the byte range in a range/content-range (the 0 in 0-1/2).
     */
    private final long startInclusive;

    /**
     * The end of the byte range in a range/content-range (the 1 in 0-1/2).
     */
    private final long endInclusive;

    /**
     * The total number of bytes in a content-range (the 2 in 0-1/2).
     */
    private final Long size;

    /**
     * Value object representing a {@link org.apache.http.HttpHeaders#RANGE}.
     */
    static final class Request extends HttpRange {

        Request(final long startInclusive,
                final long endInclusive) {
            super(startInclusive, endInclusive, null);
        }

        @Override
        boolean matches(final HttpRange other) {
            notNull(other, "Compared HttpRange must not be null");

            return this.getStartInclusive() == other.startInclusive
                    && this.getEndInclusive() == other.endInclusive;
        }

        @Override
        String render() {
            return String.format("bytes=%d-%d", this.getStartInclusive(), this.getEndInclusive());
        }

        @Override
        public String toString() {
            return "HttpRange.Request{"
                    + "startInclusive=" + this.getStartInclusive()
                    + ", endInclusive=" + this.getEndInclusive()
                    + '}';
        }
    }

    /**
     * Value object representing a {@link org.apache.http.HttpHeaders#CONTENT_RANGE}.
     */
    static final class Response extends HttpRange {

        Response(final long startInclusive,
                 final long endInclusive,
                 final long size) {
            super(startInclusive, endInclusive, size);
        }

        @Override
        boolean matches(final HttpRange other) {
            notNull(other, "Compared HttpRange must not be null");

            return this.getStartInclusive() == other.startInclusive
                    && this.getEndInclusive() == other.endInclusive
                    && Objects.equals(this.getSize(), other.size);
        }

        @Override
        String render() {
            return String.format("bytes %d-%d/%d", this.getStartInclusive(), this.getEndInclusive(), this.getSize());
        }

        @Override
        public String toString() {
            return "HttpRange.Response{"
                    + "startInclusive=" + this.getStartInclusive()
                    + ", endInclusive=" + this.getEndInclusive()
                    + ", size=" + this.getSize()
                    + '}';
        }
    }

    HttpRange(final long startInclusive,
              final long endInclusive,
              final Long size) {
        if (startInclusive > endInclusive) {
            throw new IllegalArgumentException("<range-start> must not be greater than <range-end>");
        }

        if (null != size) {
            if (size < 0) {
                throw new IllegalArgumentException("<range-size> must not be less than zero");
            }

            if ((1 + endInclusive - startInclusive) > size) {
                throw new IllegalArgumentException(
                        "<range-size> must not be less than the bytes defined by <range-start> and <range-end>");
            }
        }

        this.startInclusive = startInclusive;
        this.endInclusive = endInclusive;
        this.size = size;
    }

    long getStartInclusive() {
        return this.startInclusive;
    }

    long getEndInclusive() {
        return this.endInclusive;
    }

    Long getSize() {
        return this.size;
    }

    long contentLength() {
        return 1 + this.endInclusive - this.startInclusive;
    }

    /**
     * Constructs a range from a {@code Content-Length}, assuming the entire object is being requested.
     *
     * @param contentLength the object's Content-Length
     * @return a range representing the object content
     */
    @SuppressWarnings("unchecked")
    static <T extends HttpRange> T fromContentLength(final Class<T> klass, final long contentLength) {
        if (klass.equals(Request.class)) {
            return (T) new Request(0, contentLength - 1);
        }

        if (klass.equals(Response.class)) {
            return (T) new Response(0, contentLength - 1, contentLength);
        }

        throw new IllegalArgumentException(String.format("Cannot recognize HttpRange class: [%s]", klass));
    }

    /**
     * Checks if an HttpRange is "equivalent" to the compared HttpRange. Subclasses should compare as much as possible
     * but not fail the comparison if a property is not used by both.
     *
     * @param otherRange the comparison range
     * @return whether or not the ranges represent the same bytes
     */
    abstract boolean matches(HttpRange otherRange);

    /**
     * Render the HttpRange into its corresponding header value.
     *
     * @return the string representation of the range
     */
    abstract String render();

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null) {
            return false;
        }

        if (!this.getClass().equals(o.getClass())) {
            return false;
        }

        final HttpRange that = (HttpRange) o;

        return new EqualsBuilder()
                .append(this.startInclusive, that.startInclusive)
                .append(this.endInclusive, that.endInclusive)
                .append(this.size, that.size)
                .build();
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.startInclusive, this.endInclusive, this.size);
    }

    @Override
    public abstract String toString();

    /**
     * Deserialize a request range.
     *
     * @param requestRange the string representation of the range
     * @return a {@link Request} range
     * @throws HttpException if the range could not be parsed
     */
    static Request parseRequestRange(final String requestRange) throws HttpException {
        notNull(requestRange, "Request Range must not be null");

        if (!requestRange.matches(REGEX_REQUEST_RANGE)) {
            final String message = String.format(
                    "Invalid Range format, expected: "
                            + "[bytes <range-startInclusive>-<range-endInclusive>], got: %s",
                    requestRange);
            throw new HttpException(message);
        }

        final String[] boundsAndSize = StringUtils.split(StringUtils.removeStart(requestRange, "bytes="), "-");
        if (boundsAndSize.length != PART_COUNT_RANGE) {
            throw new HttpException(String.format("Malformed Range value, got: %s", requestRange));
        }

        return new Request(
                Long.parseUnsignedLong(boundsAndSize[0]),
                Long.parseUnsignedLong(boundsAndSize[1]));
    }

    /**
     * Deserialize a response (Content-Range) range.
     *
     * @param contentRange the string representation of the range
     * @return a {@link Request} range
     * @throws HttpException if the range could not be parsed
     */
    static Response parseContentRange(final String contentRange) throws HttpException {
        notNull(contentRange, "Content Range must not be null");

        if (!contentRange.matches(REGEX_CONTENT_RANGE)) {
            final String message = String.format(
                    "Invalid content-range format, expected: "
                            + "[bytes <range-startInclusive>-<range-endInclusive>/<size>], got: %s",
                    contentRange);
            throw new HttpException(message);
        }

        final String[] boundsAndSize = StringUtils.split(StringUtils.removeStart(contentRange, "bytes "), "-/");
        if (boundsAndSize.length != PART_COUNT_CONTENT_RANGE) {
            throw new HttpException(String.format("Malformed Range value, got: %s", contentRange));
        }

        return new Response(
                Long.parseUnsignedLong(boundsAndSize[0]),
                Long.parseUnsignedLong(boundsAndSize[1]),
                Long.parseUnsignedLong(boundsAndSize[2]));
    }
}
