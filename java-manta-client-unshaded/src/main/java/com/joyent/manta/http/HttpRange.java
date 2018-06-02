package com.joyent.manta.http;

import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

import static org.apache.commons.lang3.Validate.notNull;
import static org.apache.commons.lang3.Validate.validState;

/**
 * A value object for the <a href="https://tools.ietf.org/html/rfc7233#section-4.2">Content-Range</a> header.
 */
class HttpRange {

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

    static class Request extends HttpRange {
        Request(final long startInclusive,
                final long endInclusive) {
            super(startInclusive, endInclusive, null);
        }

        public void validateResponse(final HttpRange.Response contentRange) {

            final boolean matches =
                    this.getStartInclusive() == contentRange.getStartInclusive() &&
                    this.getEndInclusive() == contentRange.getEndInclusive();

            // TODO: complain correctly
            validState(matches, "Blah blah");
        }


        @Override
        public String toString() {
            return "HttpRange.Request{" +
                    "startInclusive=" + this.getStartInclusive() +
                    ", endInclusive=" + this.getEndInclusive() +
                    '}';
        }
    }

    static class Response extends HttpRange {
        Response(final long startInclusive,
                 final long endInclusive,
                 final long size) {
            super(startInclusive, endInclusive, size);
        }

        @Override
        public String toString() {
            return "HttpRange.Response{" +
                    "startInclusive=" + this.getStartInclusive() +
                    ", endInclusive=" + this.getEndInclusive() +
                    ", size=" + this.getSize() +
                    '}';
        }
    }

    static class Goal extends Response {
        Goal(final long startInclusive,
             final long endInclusive,
             final long size) {
            super(startInclusive, endInclusive, size);
        }
    }

    HttpRange(final long startInclusive,
              final long endInclusive,
              final Long size) {
        // TODO: can these be equal?
        validState(startInclusive <= endInclusive);

        if (null != size) {
            validState(0 < size);
            validState((1 + endInclusive - startInclusive) <= size);
        }

        this.startInclusive = startInclusive;
        this.endInclusive = endInclusive;
        this.size = size;
    }

    public long getStartInclusive() {
        return this.startInclusive;
    }

    public long getEndInclusive() {
        return this.endInclusive;
    }

    public Long getSize() {
        return this.size;
    }

    public static Request parseRequestRange(final String requestRange) {
        notNull(requestRange, "Request Range must not be null");

        if (!requestRange.matches("bytes=[0-9]+-[0-9]+")) {
            final String message = String.format(
                    "Invalid content-range format, expected: [bytes <range-startInclusive>-<range-endInclusive>], got: %s",
                    requestRange);
            throw new IllegalArgumentException(message);
        }

        final String[] boundsAndSize = StringUtils.split(StringUtils.removeStart(requestRange, "bytes="), "-");
        validState(boundsAndSize.length == 2, "Unexpected content-range parts");

        return new Request(
                Long.parseUnsignedLong(boundsAndSize[0]),
                Long.parseUnsignedLong(boundsAndSize[1]));
    }

    public static Response parseContentRange(final String contentRange) {
        notNull(contentRange, "Content Range must not be null");

        if (!contentRange.matches("bytes [0-9]+-[0-9]+/[0-9]+")) {
            final String message = String.format(
                    "Invalid content-range format, expected: [bytes <range-startInclusive>-<range-endInclusive>/<size>], got: %s",
                    contentRange);
            throw new IllegalArgumentException(message);
        }

        final String[] boundsAndSize = StringUtils.split(StringUtils.removeStart(contentRange, "bytes "), "-/");
        validState(boundsAndSize.length == 3, "Unexpected content-range parts");

        return new Response(
                Long.parseUnsignedLong(boundsAndSize[0]),
                Long.parseUnsignedLong(boundsAndSize[1]),
                Long.parseUnsignedLong(boundsAndSize[2]));
    }

    public String renderRequestRange() {
        final StringBuilder sb = new StringBuilder("bytes=");
        sb.append(startInclusive);
        sb.append("-");
        sb.append(endInclusive);

        return sb.toString();
    }

    // TODO: I don't think we need this
    // public String renderContentRange() {
    //     final StringBuilder sb = new StringBuilder("bytes ");
    //     sb.append(startInclusive);
    //     sb.append("-");
    //     sb.append(endInclusive);
    //     sb.append("/");
    //     sb.append(size);
    //
    //     return sb.toString();
    // }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }

        final HttpRange that = (HttpRange) o;

        return this.startInclusive == that.startInclusive &&
                this.endInclusive == that.endInclusive &&
                // TODO: there has to be a better way to express this safely
                (this.size != null && this.size.equals(that.size)
                        ||
                        that.size != null && that.size.equals(this.size));
    }

    @Override
    public int hashCode() {
        return Objects.hash(startInclusive, endInclusive, size);
    }

    @Override
    public String toString() {
        return "HttpRange{" +
                "startInclusive=" + this.startInclusive +
                ", endInclusive=" + this.endInclusive +
                ", size=" + this.size +
                '}';
    }
}
