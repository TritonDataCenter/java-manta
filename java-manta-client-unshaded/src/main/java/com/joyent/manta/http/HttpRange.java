package com.joyent.manta.http;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpException;

import java.util.Objects;

import static org.apache.commons.lang3.Validate.notNull;
import static org.apache.commons.lang3.Validate.validState;

/**
 * A value object for the <a href="https://tools.ietf.org/html/rfc7233#section-4.2">Content-Range</a> header.
 */
abstract class HttpRange {

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

        // do we need a new exception type for this?
        void validateResponseRange(final HttpRange.Response contentRange) throws HttpException {
            if (this.getStartInclusive() != contentRange.getStartInclusive()) {
                final String message = String.format(
                        "Unexpected start of Content-Range: expected [%d], got [%d]",
                        this.getStartInclusive(),
                        contentRange.getStartInclusive());

                throw new HttpException(message);
            }

            if (this.getEndInclusive() != contentRange.getEndInclusive()) {
                final String message = String.format(
                        "Unexpected end of Content-Range: expected [%d], got [%d]",
                        this.getEndInclusive(),
                        contentRange.getEndInclusive());
                throw new HttpException(message);
            }
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

    static class Goal extends HttpRange {
        Goal(final long startInclusive,
             final long endInclusive,
             final long size) {
            super(startInclusive, endInclusive, size);
        }

        void validateResponseRange(final HttpRange.Response contentRange) {

            if (this.getEndInclusive() != contentRange.getEndInclusive()) {
                final String message = String.format(
                        "Unexpected end of Content-Range: expected [%d], got [%d]",
                        this.getEndInclusive(),
                        contentRange.getEndInclusive());
                throw new IllegalArgumentException(message);
            }

            if (!this.getSize().equals(contentRange.getSize())) {
                final String message = String.format(
                        "Unexpected size of Content-Range: expected [%d], got [%d]",
                        this.getSize(),
                        contentRange.getSize());
                throw new IllegalArgumentException(message);
            }
        }

        @Override
        public String toString() {
            return "HttpRange.Goal{" +
                    "startInclusive=" + this.getStartInclusive() +
                    ", endInclusive=" + this.getEndInclusive() +
                    ", size=" + this.getSize() +
                    '}';
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

    long getStartInclusive() {
        return this.startInclusive;
    }

    long getEndInclusive() {
        return this.endInclusive;
    }

    Long getSize() {
        return this.size;
    }

    static Response parseContentRange(final String contentRange) throws HttpException {
        notNull(contentRange, "Content Range must not be null");

        if (!contentRange.matches("bytes [0-9]+-[0-9]+/[0-9]+")) {
            final String message = String.format(
                    "Invalid content-range format, expected: [bytes <range-startInclusive>-<range-endInclusive>/<size>], got: %s",
                    contentRange);
            throw new HttpException(message);
        }

        final String[] boundsAndSize = StringUtils.split(StringUtils.removeStart(contentRange, "bytes "), "-/");
        validState(boundsAndSize.length == 3, "Unexpected content-range parts");

        return new Response(
                Long.parseUnsignedLong(boundsAndSize[0]),
                Long.parseUnsignedLong(boundsAndSize[1]),
                Long.parseUnsignedLong(boundsAndSize[2]));
    }

    String renderRequestRange() {
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
    public abstract String toString();
}
