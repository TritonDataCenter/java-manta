package com.joyent.manta.http;

import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

import static org.apache.commons.lang3.Validate.notNull;
import static org.apache.commons.lang3.Validate.validState;

/**
 * A value object for the <a href="https://tools.ietf.org/html/rfc7233#section-4.2">Content-Range</a> header.
 */
class HttpRange {

    final long start;
    final long end;
    final Long size;

    HttpRange(final long start,
                     final long end,
                     final long size) {
        validState(0 < size);
        validState(start < end);
        validState((1 + end - start) <= size);

        this.start = start;
        this.end = end;
        this.size = size;
    }

    HttpRange(final long start,
            final long end) {
        validState(start < end);

        this.start = start;
        this.end = end;
        this.size = null;
    }

    public static HttpRange parseRequestRange(final String requestRange) {
        notNull(requestRange, "Request Range must not be null");

        if (!requestRange.matches("bytes=[0-9]+-[0-9]+")) {
            final String message = String.format(
                    "Invalid content-range format, expected: [bytes <range-start>-<range-end>], got: %s",
                    requestRange);
            throw new IllegalArgumentException(message);
        }

        final String[] boundsAndSize = StringUtils.split(StringUtils.removeStart(requestRange, "bytes="), "-");
        validState(boundsAndSize.length == 2, "Unexpected content-range parts");

        return new HttpRange(
                Long.parseUnsignedLong(boundsAndSize[0]),
                Long.parseUnsignedLong(boundsAndSize[1]));
    }

    public static HttpRange parseContentRange(final String contentRange) {
        notNull(contentRange, "Content Range must not be null");

        if (!contentRange.matches("bytes [0-9]+-[0-9]+/[0-9]+")) {
            final String message = String.format(
                    "Invalid content-range format, expected: [bytes <range-start>-<range-end>/<size>], got: %s",
                    contentRange);
            throw new IllegalArgumentException(message);
        }

        final String[] boundsAndSize = StringUtils.split(StringUtils.removeStart(contentRange, "bytes "), "-/");
        validState(boundsAndSize.length == 3, "Unexpected content-range parts");

        return new HttpRange(
                Long.parseUnsignedLong(boundsAndSize[0]),
                Long.parseUnsignedLong(boundsAndSize[1]),
                Long.parseUnsignedLong(boundsAndSize[2]));
    }

    public String renderRequestRange() {
        final StringBuilder sb = new StringBuilder("bytes=");
        sb.append(start);
        sb.append("-");
        sb.append(end);

        return sb.toString();
    }

    // TODO: I don't think we need this
    public String renderContentRange() {
        final StringBuilder sb = new StringBuilder("bytes ");
        sb.append(start);
        sb.append("-");
        sb.append(end);
        sb.append("/");
        sb.append(size);

        return sb.toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final HttpRange that = (HttpRange) o;
        return this.start == that.start &&
                this.end == that.end &&
                this.size == that.size;
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end, size);
    }

    @Override
    public String toString() {
        return "HttpRange{" +
                "start=" + this.start +
                ", end=" + this.end +
                ", size=" + this.size +
                '}';
    }
}
