/**
 * Copyright (c) 2013, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.util.Key;
import org.apache.http.impl.cookie.DateParseException;
import org.apache.http.impl.cookie.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Objects;

import static com.joyent.http.signature.HttpSignerUtils.X_REQUEST_ID_HEADER;


/**
 * A Manta storage object.
 * <p>
 * I/O is performed via the getDataInputStream() and setDataInputStream() methods. Importantly, the stream isn't
 * automatically closed, specifically, the http connection remains open until the stream is closed -- so consumers must
 * call close() when done to avoid memory leaks. Example get usage:
 * </p>
 *
 * <pre>
 * MantaClient client = MantaClient.getInstance(...);
 * MantaObject object = client.get(&quot;/user/stor/foo&quot;);
 * // MantaUtils.inputStreamToString() closes the inputstream.
 * String data = MantaUtils.inputStreamToString(gotObject.getDataInputStream());
 * </pre>
 * <p>
 * Example put usage:
 * </p>
 *
 * <pre>
 * MantaClient client = MantaClient.getInstance(...);
 * MantaObject object = new MantaObject(&quot;user/stor/foo&quot;);
 * object.setHeader("durability-level", 3);
 * InputStream is = new FileInputStream(new File(TEST_FILE));
 * object.setDataInputStream(is);
 * client.put(object, null);
 * </pre>
 *
 * @author Yunong Xiao
 */
public class MantaObjectMetadata implements MantaObject {

    private static final long serialVersionUID = -5690762948837343786L;

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
     * The http headers associated with this object.
     */
    private HttpHeaders httpHeaders;


    /**
     * Empty constructor for the JSON parser.
     */
    public MantaObjectMetadata() {
    }


    /**
     * Creates a MantaObject.
     *
     * @param path The fully qualified path of the object in Manta. i.e. "/user/stor/path/to/some/file/or/dir".
     */
    public MantaObjectMetadata(final String path) {
        if (path == null) {
            throw new IllegalArgumentException("Path must be present");
        }

        this.path = path;
        this.httpHeaders = new HttpHeaders();
    }


    /**
     * Creates a MantaObject.
     *
     * @param path The fully qualified path of the object in Manta. i.e. "/user/stor/path/to/some/file/or/dir".
     * @param headers Optional {@link HttpHeaders}. Use this to set any additional headers on the Manta object.  For the
     *                full list of Manta headers see the <a href="http://apidocs.joyent.com/manta/manta/">Manta API</a>.
     */
    public MantaObjectMetadata(final String path, final HttpHeaders headers) {
        if (path == null) {
            throw new IllegalArgumentException("Path must be present");
        }
        if (headers == null) {
            throw new IllegalArgumentException("Headers must be present");
        }

        this.path = path;
        this.httpHeaders = headers;
        this.contentLength = headers.getContentLength();
        this.etag = headers.getETag();
        this.mtime = headers.getLastModified();
        this.requestId = headers.getFirstHeaderStringValue(X_REQUEST_ID_HEADER);

        if (headers.getContentType().equals(DIRECTORY_RESPONSE_CONTENT_TYPE)) {
            this.type ="directory";
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


    /**
     * Sets the content length (size) value.
     *
     * @param contentLength the content length (size) value.
     */
    public void setContentLength(final Long contentLength) {
        this.contentLength = contentLength;
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
     * Sets the etag value.
     *
     * @param etag the mtime value.
     */
    public void setEtag(final String etag) {
        this.etag = etag;
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

        try {
            return DateUtils.parseDate(lastModified);
        } catch (DateParseException e) {
            Logger logger = LoggerFactory.getLogger(getClass());
            logger.warn(String.format("Error parsing mtime value: %s", lastModified), e);

            return null;
        }
    }

    /**
     * Sets the mtime value (last modified time) from a {@link java.util.Date} object.
     * @param lastModified null or a Date object
     */
    public void setLastModifiedTime(final Date lastModified) {
        if (lastModified == null) {
            this.mtime = null;
            return;
        }

        this.mtime = DateUtils.formatDate(lastModified);
    }

    @Override
    public String getType() {
        return type;
    }


    /**
     * Sets the type value.
     *
     * @param type the type value.
     */
    public void setType(final String type) {
        this.type = type;
    }


    @Override
    public final HttpHeaders getHttpHeaders() {
        return this.httpHeaders;
    }


    /**
     * Sets the {@link com.google.api.client.http.HttpHeaders} in this object. Note any previous headers will be lost.
     * For the full list of Manta headers see the <a href="http://apidocs.joyent.com/manta/manta/">Manta API</a>.
     *
     * @param httpHeaders the httpHeaders to set.
     */
    public final void setHttpHeaders(final HttpHeaders httpHeaders) {
        this.httpHeaders = httpHeaders;
    }


    @Override
    public final Object getHeader(final String fieldName) {
        return this.httpHeaders.get(fieldName);
    }


    /**
     * Sets custom headers on the Manta object. This really just delegates to setting the
     * {@link com.google.api.client.http.HttpHeaders} object.  For the full list of Manta headers see the
     * <a href="http://apidocs.joyent.com/manta/manta/">Manta API</a>.
     *
     * @param fieldName the field name.
     * @param value the field value.
     */
    public final void setHeader(final String fieldName, final Object value) {
        this.httpHeaders.set(fieldName, value);
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
        MantaObjectMetadata that = (MantaObjectMetadata)o;
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
