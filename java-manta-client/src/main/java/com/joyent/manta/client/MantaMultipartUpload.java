package com.joyent.manta.client;

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

    /**
     * A single part of a multipart upload.
     */
    public static class Part implements Comparable<Part>, Serializable {
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
         */
        public Part(final int partNumber, final String objectPath,
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
        Part(final MantaObject object) {
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
        public int compareTo(final Part that) {
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

            final Part part = (Part) that;
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
}
