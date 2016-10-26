package com.joyent.manta.client.multipart;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;
import java.util.UUID;

/**
 * Class representing a multipart upload in progress.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 2.5.0
 */
public class MantaMultipartUpload implements Comparator<MantaMultipartUpload>, Serializable {
    private static final long serialVersionUID = 1993692738555250766L;

    /**
     * Transaction ID for multipart upload.
     */
    private UUID id;

    /**
     * Path to final object being uploaded to Manta.
     */
    private String path;

    /**
     * Creates a new instance.
     *
     * @param uploadId Transaction ID for multipart upload
     * @param path Path to final object being uploaded to Manta
     */
    public MantaMultipartUpload(final UUID uploadId, final String path) {
        this.id = uploadId;
        this.path = path;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final MantaMultipartUpload that = (MantaMultipartUpload) o;

        return Objects.equals(id, that.id)
               && Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, path);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("path", path)
                .toString();
    }

    @Override
    public int compare(final MantaMultipartUpload o1, final MantaMultipartUpload o2) {
        return o1.path.compareTo(o2.path);
    }

    public UUID getId() {
        return id;
    }

    public String getPath() {
        return path;
    }
}
