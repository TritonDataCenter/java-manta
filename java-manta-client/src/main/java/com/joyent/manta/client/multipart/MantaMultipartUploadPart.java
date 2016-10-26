package com.joyent.manta.client.multipart;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.client.MantaUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.Objects;


/**
 * A single part of a multipart upload.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 2.5.0
 */
public class MantaMultipartUploadPart
        implements Comparable<MantaMultipartUploadPart>, Serializable {
    private static final long serialVersionUID = -738331736064518314L;

    /**
     * Non-zero positive integer representing the relative position of the
     * part in relation to the other parts for the multipart upload.
     */
    private final int partNumber;

    /**
     * Remote path on Manta for the part's file.
     */
    private final String objectPath;

    /**
     * Etag value of the part.
     */
    private final String etag;

    /**
     * Content length of the part.
     */
    private final Long length;

    /**
     * Creates a new instance based on explicitly defined parameters.
     *
     * @param partNumber Non-zero positive integer representing the relative position of the part
     * @param objectPath Remote path on Manta for the part's file
     * @param etag Etag value of the part
     * @param length size in bytes of the part
     */
    public MantaMultipartUploadPart(final int partNumber, final String objectPath,
                final String etag, final Long length) {
        this.partNumber = partNumber;
        this.objectPath = objectPath;
        this.etag = etag;
        this.length = length;
    }

    /**
     * Creates a new instance based on a response from {@link MantaClient}.
     *
     * @param object response object from returned from {@link MantaClient}
     */
    public MantaMultipartUploadPart(final MantaObject object) {
        final String filename = MantaUtils.lastItemInPath(object.getPath());
        this.objectPath = object.getPath();
        this.partNumber = Integer.parseInt(filename);
        this.etag = object.getEtag();
        length = object.getContentLength();
    }

    public int getPartNumber() {
        return partNumber;
    }

    public String getEtag() {
        return etag;
    }

    protected String getObjectPath() {
        return objectPath;
    }

    @Override
    public int compareTo(final MantaMultipartUploadPart that) {
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

        final MantaMultipartUploadPart part = (MantaMultipartUploadPart) that;
        return partNumber == part.partNumber
                && Objects.equals(objectPath, part.objectPath)
                && Objects.equals(etag, part.etag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partNumber, objectPath, etag);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("partNumber", partNumber)
                .append("objectPath", objectPath)
                .append("etag", etag)
                .toString();
    }
}
