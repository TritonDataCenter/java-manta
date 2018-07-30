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
import org.apache.http.ProtocolException;

import java.util.Objects;

import static org.apache.commons.lang3.Validate.notNull;

/**
 * A value object for <a href="https://tools.ietf.org/html/rfc7233#section-3">Range</a> and
 * <a href="https://tools.ietf.org/html/rfc7233#section-4.2">Content-Range</a> headers.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 * @since 3.2.2
 */
public abstract class HttpRange {

    /**
     * The regex used to validateResponseWithMarker request Range header values that are bounded.
     */
    private static final String REGEX_BOUNDED_REQUEST_RANGE = "bytes=[0-9]+-[0-9]+";

    /**
     * The regex used to validateResponseWithMarker request Range header values that are unbounded (and allowed by the
     * HTTP spec).
     */
    private static final String REGEX_UNBOUNDED_REQUEST_RANGE = "bytes=[0-9]+-";

    /**
     * The regex used to validateResponseWithMarker Content-Range header values.
     */
    private static final String REGEX_CONTENT_RANGE = "bytes [0-9]+-[0-9]+/[0-9]+";

    /**
     * The number of segments the unbounded Range regex expects to find.
     */
    private static final int PART_COUNT_UNBOUNDED_RANGE = 1;

    /**
     * The number of segments the bounded Range regex expects to find.
     */
    private static final int PART_COUNT_BOUNDED_RANGE = 2;

    /**
     * The number of segments the Content-Range regex expects to find.
     */
    private static final int PART_COUNT_CONTENT_RANGE = 3;

    /**
     * Exception message for issues with parsing request ranges.
     */
    private static final String FMT_ERROR_MATCHING_REQUEST_RANGE =
            "Invalid Range format, expected one of: "
                    + "["
                    + "bytes=<startInclusive>-<endInclusive>"
                    + ","
                    + "bytes=<startInclusive>-"
                    + "], got: %s";

    /**
     * The start of the byte range in a range/content-range (the 0 in 0-1/2).
     */
    private final long startInclusive;

    /**
     * The end of the byte range in a range/content-range (the 1 in 0-1/2).
     */
    private final Long endInclusive;

    /**
     * The total number of bytes in a content-range (the 2 in 0-1/2).
     */
    private final Long size;

    /**
     * Abstract class grouping request ranges.
     */
    abstract static class Request extends HttpRange {

        Request(final long startInclusive, final Long endInclusive, final Long size) {
            super(startInclusive, endInclusive, size);
        }
    }

    /**
     * Value object representing a {@link org.apache.http.HttpHeaders#RANGE}.
     */
    public static final class UnboundedRequest extends Request {

        /**
         * Construct a {@code Range} range, encapsulating the start of the range.
         *
         * @param startInclusive inclusive byte range start offset
         */
        public UnboundedRequest(final long startInclusive) {
            super(startInclusive, null, null);
        }

        @Override
        protected boolean matches(final HttpRange other) {
            notNull(other, "Compared HttpRange must not be null");

            return Objects.equals(this.getStartInclusive(), other.startInclusive);
        }

        @Override
        public String render() {
            return String.format("bytes=%d-", this.getStartInclusive());
        }

        @Override
        public String toString() {
            return "HttpRange.UnboundedRequest{"
                    + "startInclusive=" + this.getStartInclusive()
                    + ", endInclusive=" + this.getEndInclusive()
                    + '}';
        }
    }

    /**
     * Value object representing a {@link org.apache.http.HttpHeaders#RANGE}.
     */
    public static final class BoundedRequest extends Request {

        /**
         * Construct a {@code Range} range, encapsulating the start and end of the range.
         *
         * @param startInclusive inclusive byte range start offset
         * @param endInclusive inclusive byte range end offset
         */
        public BoundedRequest(final long startInclusive,
                              final long endInclusive) {
            super(startInclusive, endInclusive, null);
        }

        @Override
        protected boolean matches(final HttpRange other) {
            notNull(other, "Compared HttpRange must not be null");

            final boolean matchingStart = Objects.equals(this.getStartInclusive(), other.startInclusive);
            final boolean matchingEnd = Objects.equals(this.getEndInclusive(), other.endInclusive);

            if (other.endInclusive != null) {
                return matchingStart && matchingEnd;
            }

            return matchingStart;
        }

        @Override
        public String render() {
            return String.format("bytes=%d-%d", this.getStartInclusive(), this.getEndInclusive());
        }

        @Override
        public String toString() {
            return "HttpRange.BoundedRequest{"
                    + "startInclusive=" + this.getStartInclusive()
                    + ", endInclusive=" + this.getEndInclusive()
                    + '}';
        }
    }

    /**
     * Value object representing a {@link org.apache.http.HttpHeaders#CONTENT_RANGE}.
     */
    public static final class Response extends HttpRange {

        /**
         * Construct a {@code Content-Range} range, encapsulating the start and end of the range
         * in addition to the total object size (independent of the size of the range being downloaded).
         *
         * @param startInclusive inclusive byte range start offset
         * @param endInclusive inclusive byte range end offset
         * @param size total object size
         */
        public Response(final long startInclusive,
                        final long endInclusive,
                        final long size) {
            super(startInclusive, endInclusive, size);
        }

        @Override
        protected boolean matches(final HttpRange other) {
            notNull(other, "Compared HttpRange must not be null");

            final boolean matchingStart = Objects.equals(this.getStartInclusive(), other.startInclusive);
            final boolean matchingEnd = Objects.equals(this.getEndInclusive(), other.endInclusive);
            final boolean matchingSize = Objects.equals(this.getSize(), other.size);

            if (other.size == null) {
                if (other.endInclusive == null) {
                    return matchingStart;
                }

                return matchingStart && matchingEnd;
            }

            return matchingStart && matchingEnd && matchingSize;
        }

        @Override
        public String render() {
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
              final Long endInclusive,
              final Long size) {
        if (null != endInclusive && startInclusive > endInclusive) {
            throw new IllegalArgumentException("<start> must not be greater than <end>");
        }

        if (size != null) {
            if (endInclusive == null) {
                throw new IllegalArgumentException("<end> must be present if size is present");
            }

            if (size < 0) {
                throw new IllegalArgumentException("<size> must not be less than zero");
            }

            if (size < (1 + endInclusive - startInclusive)) {
                throw new IllegalArgumentException(
                        "<size> must not be less than the bytes defined by <start> and <end>");
            }

            // size can be greater than the range if only a part of the file was requested
        }

        this.startInclusive = startInclusive;
        this.endInclusive = endInclusive;
        this.size = size;
    }

    long getStartInclusive() {
        return this.startInclusive;
    }

    Long getEndInclusive() {
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
        if (klass.equals(BoundedRequest.class)) {
            return (T) new BoundedRequest(0, contentLength - 1);
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
    protected abstract boolean matches(HttpRange otherRange);

    /**
     * Render the HttpRange into its corresponding header value.
     *
     * @return the string representation of the range
     */
    public abstract String render();

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
     * @return a {@link BoundedRequest} range
     * @throws ProtocolException if the range could not be parsed
     */
    static Request parseRequestRange(final String requestRange) throws ProtocolException {
        notNull(requestRange, "Request Range must not be null");

        final boolean boundedMatch = requestRange.matches(REGEX_BOUNDED_REQUEST_RANGE);
        final boolean unboundedMatch = requestRange.matches(REGEX_UNBOUNDED_REQUEST_RANGE);
        if (!boundedMatch && !unboundedMatch) {
            throw new ProtocolException(String.format(FMT_ERROR_MATCHING_REQUEST_RANGE, requestRange));
        }

        final String[] bounds = StringUtils.split(StringUtils.removeStart(requestRange, "bytes="), "-");
        if (boundedMatch) {
            if (bounds.length != PART_COUNT_BOUNDED_RANGE) {
                throw new ProtocolException(String.format(FMT_ERROR_MATCHING_REQUEST_RANGE, requestRange));
            }

            return new BoundedRequest(
                    Long.parseUnsignedLong(bounds[0]),
                    Long.parseUnsignedLong(bounds[1]));
        }

        if (unboundedMatch && bounds.length != PART_COUNT_UNBOUNDED_RANGE) {
            throw new ProtocolException(String.format(FMT_ERROR_MATCHING_REQUEST_RANGE, requestRange));
        }

        return new UnboundedRequest(Long.parseUnsignedLong(bounds[0]));
    }

    /**
     * Deserialize a response (Content-Range) range.
     *
     * @param contentRange the string representation of the range
     * @return a {@link BoundedRequest} range
     * @throws ProtocolException if the range could not be parsed
     */
    static Response parseContentRange(final String contentRange) throws ProtocolException {
        notNull(contentRange, "Content Range must not be null");

        if (!contentRange.matches(REGEX_CONTENT_RANGE)) {
            final String message = String.format(
                    "Invalid content-range format, expected: "
                            + "[bytes <startInclusive>-<endInclusive>/<size>], got: %s",
                    contentRange);
            throw new ProtocolException(message);
        }

        final String[] boundsAndSize = StringUtils.split(StringUtils.removeStart(contentRange, "bytes "), "-/");
        if (boundsAndSize.length != PART_COUNT_CONTENT_RANGE) {
            throw new ProtocolException(String.format("Malformed Range value, got: %s", contentRange));
        }

        return new Response(
                Long.parseUnsignedLong(boundsAndSize[0]),
                Long.parseUnsignedLong(boundsAndSize[1]),
                Long.parseUnsignedLong(boundsAndSize[2]));
    }
}
