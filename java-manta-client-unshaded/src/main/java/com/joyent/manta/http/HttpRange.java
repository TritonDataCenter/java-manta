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
    final long startInclusive;

    /**
     * The end of the byte range in a range/content-range (the 1 in 0-1/2).
     */
    final long endInclusive;

    /**
     * The total number of bytes in a content-range (the 2 in 0-1/2).
     */
    final Long size;


    /*

    TODO: maybe?

    static class Sized extends HttpRange {

        Sized(final long startInclusive,
              final long endInclusive,
              final Long size) {
            super(startInclusive, endInclusive);
            this.size = size;
        }
    }
    */

    static class Request extends HttpRange {
        Request(final long startInclusive,
                final long endInclusive) {
            super(startInclusive, endInclusive, null);
        }

        // do we need a new exception type for this?
        boolean matches(final HttpRange other) {
            notNull(other, "Compared HttpRange must not be null");

            return this.startInclusive == other.startInclusive
                    && this.endInclusive == other.endInclusive;
        }

        @Override
        String render() {
            return String.format("bytes=%d-%d", this.startInclusive, this.endInclusive);
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

        boolean matches(final HttpRange other) {
            notNull(other, "Compared HttpRange must not be null");

            return this.startInclusive == other.startInclusive
                    && this.endInclusive == other.endInclusive
                    && this.size.equals(other.size);
        }

        @Override
        String render() {
            return String.format("bytes %d-%d/%d", this.startInclusive, this.endInclusive, this.size);
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

        boolean matches(final HttpRange other) {
            notNull(other, "Compared HttpRange must not be null");

            return this.endInclusive == other.endInclusive
                    && this.size.equals(other.size);
        }

        @Override
        String render() {
            return String.format("bytes %d-%d/%d", this.startInclusive, this.endInclusive, this.size);
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

    static Request parseRequestRange(final String requestRange) throws HttpException {
        notNull(requestRange, "Request Range must not be null");

        if (!requestRange.matches("bytes=[0-9]+-[0-9]+")) {
            final String message = String.format(
                    "Invalid Range format, expected: [bytes <range-startInclusive>-<range-endInclusive>], got: %s",
                    requestRange);
            throw new HttpException(message);
        }

        final String[] boundsAndSize = StringUtils.split(StringUtils.removeStart(requestRange, "bytes="), "-");
        if (boundsAndSize.length != 2) {
            throw new HttpException(String.format("Malformed Range value, got: %s", requestRange));
        }

        return new Request(
                Long.parseUnsignedLong(boundsAndSize[0]),
                Long.parseUnsignedLong(boundsAndSize[1]));
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

    abstract boolean matches(HttpRange otherRange);

    abstract String render();

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
