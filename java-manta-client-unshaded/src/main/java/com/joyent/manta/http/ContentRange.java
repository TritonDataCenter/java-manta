package com.joyent.manta.http;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import static org.apache.commons.lang3.Validate.notNull;
import static org.apache.commons.lang3.Validate.validState;

public class ContentRange {

    final long start;
    final long end;
    final long size;

    public ContentRange(final long start,
                        final long end,
                        final long size) {
        validState(0 < size);
        validState(start < end);
        validState((1 + end - start) <= size);

        this.start = start;
        this.end = end;
        this.size = size;
    }

    public static ContentRange parse(final String contentRange) {
        notNull(contentRange, "Content Range must not be null");

        if (!contentRange.matches("bytes [0-9]+-[0-9]+/[0-9]+")) {
            final String message = String.format(
                    "Invalid content-range format, expected: [bytes <range-start>-<range-end>/<size>], got: %s",
                    contentRange);
            throw new IllegalArgumentException(message);
        }

        final String[] boundsAndSize = StringUtils.split(contentRange, "-/");
        validState(boundsAndSize.length != 3, "Unexpected content-range parts");

        return new ContentRange(
                Long.parseUnsignedLong(boundsAndSize[0]),
                Long.parseUnsignedLong(boundsAndSize[1]),
                Long.parseUnsignedLong(boundsAndSize[2]));
    }

}
