/**
 * Copyright (c) 2013, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.google.api.client.util.Key;
import org.apache.http.client.utils.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Objects;

/**
 * A Manta storage object.
 * <p>I/O is performed via the methods on the {@link MantaClient} class.</p>
 *
 * @author Yunong Xiao
 */
public class MantaObjectResponse implements MantaObject {

    private static final long serialVersionUID = 2752480890369898121L;

    /**
     * The content-type used to represent Manta directory resources in http responses.
     */
    public static final String DIRECTORY_RESPONSE_CONTENT_TYPE = "application/x-json-stream; type=directory";

    /**
     * The name value for this object.
     */
    @Key("name")
    private String path;


    /**
     * The content length (size) value for this object.
     */
    @Key("size")
    private Long contentLength;


    /**
     * The etag value for this object.
     */
    @Key("etag")
    private String etag;


    /**
     * The mtime value for this object.
     */
    @Key("mtime")
    private String mtime;


    /**
     * The type value for this object.
     */
    @Key("type")
    private String type;


    /**
     * Unique request id made to Manta.
     */
    private String requestId;


    /**
     * The HTTP headers associated with this object.
     */
    private MantaHttpHeaders httpHeaders;


    /**
     * The metadata associated with this object.
     */
    private MantaMetadata metadata;


    /**
     * Empty constructor for the JSON parser.
     */
    public MantaObjectResponse() {
    }


    /**
     * Creates a MantaObject.
     *
     * @param path The fully qualified path of the object in Manta. i.e. "/user/stor/path/to/some/file/or/dir".
     */
    public MantaObjectResponse(final String path) {
        if (path == null) {
            throw new IllegalArgumentException("Path must be present");
        }

        this.path = path;
        this.httpHeaders = new MantaHttpHeaders();
    }


    /**
     * Creates a MantaObject.
     *
     * @param path The fully qualified path of the object in Manta. i.e. "/user/stor/path/to/some/file/or/dir".
     * @param headers Optional {@link MantaHttpHeaders}. Use this to set any additional headers on the Manta object.
     *                For the full list of Manta headers see the
     *                <a href="http://apidocs.joyent.com/manta/manta/">Manta API</a>.
     */
    public MantaObjectResponse(final String path, final MantaHttpHeaders headers) {
        this(path, headers, null);
    }


    /**
     * Creates a MantaObject.
     *
     * @param path The fully qualified path of the object in Manta. i.e. "/user/stor/path/to/some/file/or/dir".
     * @param headers Optional {@link MantaHttpHeaders}. Use this to set any additional headers on the Manta object.
     *                For the full list of Manta headers see the
     *                <a href="http://apidocs.joyent.com/manta/manta/">Manta API</a>.
     * @param metadata User set metadata associated with object
     */
    public MantaObjectResponse(final String path, final MantaHttpHeaders headers, final MantaMetadata metadata) {
        Objects.requireNonNull(path, "Path must be present");
        Objects.requireNonNull(headers, "Headers must be present");

        this.path = path;
        this.httpHeaders = headers;
        this.contentLength = headers.getContentLength();
        this.etag = headers.getETag();
        this.mtime = headers.getLastModified();
        this.requestId = headers.getRequestId();

        final String contentType = headers.getContentType();
        if (contentType != null && contentType.equals(DIRECTORY_RESPONSE_CONTENT_TYPE)) {
            this.type = "directory";
        }

        if (metadata != null) {
            this.metadata = metadata;
        } else {
            this.metadata = new MantaMetadata(headers.metadataAsStrings());
        }
    }


    @Override
    public final String getPath() {
        return this.path;
    }


    /**
     * Sets the path value.
     *
     * @param path the path to set
     */
    public final void setPath(final String path) {
        this.path = path;
    }


    @Override
    public final Long getContentLength() {
        return contentLength;
    }


    @Override
    public final String getContentType() {
        String contentType = null;
        if (getHttpHeaders() != null) {
            contentType = getHttpHeaders().getContentType();
        }
        return contentType;
    }


    @Override
    public final String getEtag() {
        return etag;
    }


    /**
     * Returns the mtime value.
     *
     * @return the mtime
     */
    public final String getMtime() {
        return mtime;
    }


    /**
     * Sets the mtime value.
     *
     * @param mtime the mtime value.
     */
    public void setMtime(final String mtime) {
        this.mtime = mtime;
    }

    @Override
    public Date getLastModifiedTime() {
        final String lastModified = getMtime();
        if (lastModified == null) {
            return null;
        }

        final Date parsed = DateUtils.parseDate(lastModified);

        if (parsed == null) {
            Logger logger = LoggerFactory.getLogger(getClass());
            logger.warn("Error parsing mtime value: {}", lastModified);
        }

        return parsed;
    }


    @Override
    public String getType() {
        return type;
    }


    @Override
    public final MantaHttpHeaders getHttpHeaders() {
        return this.httpHeaders;
    }


    @Override
    public final Object getHeader(final String fieldName) {
        return this.httpHeaders.get(fieldName);
    }


    @Override
    public String getHeaderAsString(final String fieldName) {
        return this.httpHeaders.getAsString(fieldName);
    }


    @Override
    public MantaMetadata getMetadata() {
        return this.metadata;
    }

    @Override
    public final boolean isDirectory() {
        return MANTA_OBJECT_TYPE_DIRECTORY.equals(type);
    }


    @Override
    public String getRequestId() {
        return null;
    }


    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("checkstyle:designforextension")
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MantaObject)) {
            return false;
        }
        MantaObjectResponse that = (MantaObjectResponse)o;
        return Objects.equals(path, that.path)
                && Objects.equals(getContentLength(), that.getContentLength())
                && Objects.equals(getContentType(), that.getContentType())
                && Objects.equals(getEtag(), that.getEtag())
                && Objects.equals(getMtime(), that.getMtime())
                && Objects.equals(httpHeaders, that.httpHeaders);
    }


    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("checkstyle:designforextension")
    public int hashCode() {
        return Objects.hash(
                path,
                getContentLength(),
                getContentType(),
                getEtag(),
                getMtime(),
                httpHeaders);
    }


    @Override
    @SuppressWarnings("checkstyle:designforextension")
    public String toString() {
        final StringBuilder sb = new StringBuilder(getClass().getName());
        sb.append('{');
        sb.append("path='").append(path).append('\'');
        sb.append(", contentLength=").append(getContentLength());
        sb.append(", contentType='").append(getContentType()).append('\'');
        sb.append(", etag='").append(getEtag()).append('\'');
        sb.append(", mtime='").append(getMtime()).append('\'');
        sb.append(", requestId='").append(getRequestId()).append('\'');
        sb.append(", httpHeaders=").append(httpHeaders);
        sb.append(", directory=").append(isDirectory());
        sb.append('}');
        return sb.toString();
    }
}
