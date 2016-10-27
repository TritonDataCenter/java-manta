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
public class MantaMultipartUploadPart extends MantaMultipartUploadTuple
        implements Serializable {
    private static final long serialVersionUID = -738331736064518314L;

    /**
     * Remote path on Manta for the part's file.
     */
    private final String objectPath;

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

        super(partNumber, etag);
        this.objectPath = objectPath;
        this.length = length;
    }

    /**
     * Creates a new instance based on a response from {@link MantaClient}.
     *
     * @param object response object from returned from {@link MantaClient}
     */
    public MantaMultipartUploadPart(final MantaObject object) {
        super(Integer.parseInt(MantaUtils.lastItemInPath(object.getPath())),
                object.getEtag());

        this.objectPath = object.getPath();
        length = object.getContentLength();
    }

    protected String getObjectPath() {
        return objectPath;
    }

    @Override
    public boolean equals(final Object that) {
        if (this == that) {
            return true;
        }

        if (that == null || getClass() != that.getClass()) {
            return false;
        }

        if (!super.equals(that)) {
            return false;
        }

        final MantaMultipartUploadPart part = (MantaMultipartUploadPart) that;

        return Objects.equals(objectPath, part.objectPath)
               && Objects.equals(length, part.length);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), objectPath, length);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("partNumber", getPartNumber())
                .append("objectPath", getObjectPath())
                .append("etag", getEtag())
                .toString();
    }
}
