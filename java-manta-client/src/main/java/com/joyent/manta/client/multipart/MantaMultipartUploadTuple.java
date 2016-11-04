package com.joyent.manta.client.multipart;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.Objects;

/**
 * Minimal class representing the logical pairing of a ETag value and a
 * part number for a multipart upload part. This minimum set of identifying
 * values is needed when completing a Manta multipart upload.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 2.5.0
 */
public class MantaMultipartUploadTuple implements Serializable,
        Comparable<MantaMultipartUploadTuple> {
    private static final long serialVersionUID = -4050887694675747077L;

    /**
     * Non-zero positive integer representing the relative position of the
     * part in relation to the other parts for the multipart upload.
     */
    private final int partNumber;

    /**
     * HTTP Etag value returned by Manta for the multipart upload part.
     */
    private final String etag;

    /**
     * Creates a new instance based on the passed parameters.
     *
     * @param partNumber Non-zero positive integer representing the relative
     *                   position of the part in relation to the other parts for the multipart upload.
     * @param etag HTTP Etag value returned by Manta for the multipart upload part
     */
    public MantaMultipartUploadTuple(final int partNumber, final String etag) {
        if (partNumber < 1) {
            String msg = String.format("Part number must be greater than or "
                    + "equal to 1. Actual value: %d", partNumber);
            throw new IllegalArgumentException(msg);
        }

        this.partNumber = partNumber;
        this.etag = etag;
    }

    /**
     * @return An positive non-zero integer part number identifying the part's order
     */
    public int getPartNumber() {
        return this.partNumber;
    }

    /**
     * @return HTTP Etag value returned by Manta for the multipart upload part
     */
    public String getEtag() {
        return this.etag;
    }


    @Override
    public int compareTo(final MantaMultipartUploadTuple that) {
        return Integer.compare(this.getPartNumber(), that.getPartNumber());
    }

    @Override
    public boolean equals(final Object that) {
        if (this == that) {
            return true;
        }

        if (that == null || getClass() != that.getClass()) {
            return false;
        }

        MantaMultipartUploadTuple tuple = (MantaMultipartUploadTuple) that;
        return partNumber == tuple.partNumber
                && Objects.equals(etag, tuple.etag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partNumber, etag);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("partNumber", partNumber)
                .append("etag", etag)
                .toString();
    }
}
