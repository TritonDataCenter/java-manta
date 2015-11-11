/**
 * Copyright (c) 2013, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.util.Key;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Objects;


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
public class MantaObject implements Serializable {

    private static final long serialVersionUID = -3553874816118187027L;

    /**
     * The type value for data objects within the manta service.
     */
    public static final String MANTA_OBJECT_TYPE_OBJECT = "object";

    /**
     * The type value for directory objects within the manta service.
     */
    public static final String MANTA_OBJECT_TYPE_DIRECTORY = "directory";

    /**
     * Manta directory header.
     */
    public static final String DIRECTORY_HEADER = "application/x-json-stream; type=directory";

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
     * The content of the object's data as a {@link java.io.File}.
     */
    private File dataInputFile;

    /**
     * The content of the object's data as an {@link java.io.InputStream}.
     */
    private InputStream dataInputStream;

    /**
     * The content of the object's data as a {@link java.lang.String}.
     */
    private String dataInputString;


    /**
     * The http headers associated with this object.
     */
    private HttpHeaders httpHeaders;


    /**
     * Empty constructor for the JSON parser.
     */
    public MantaObject() {
    }


    /**
     * Creates a MantaObject.
     *
     * @param path The fully qualified path of the object in Manta. i.e. "/user/stor/path/to/some/file/or/dir".
     */
    public MantaObject(final String path) {
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
    public MantaObject(final String path, final HttpHeaders headers) {
        this.path = path;
        this.httpHeaders = headers;
    }


    /**
     * Returns the path value.
     *
     * @return the path
     */
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


    /**
     * Returns the content length (size) value.
     *
     * @return content length (size)
     */
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


    /**
     * Returns the content type value.
     *
     * @return the type
     */
    public final String getContentType() {
        String contentType = null;
        if (getHttpHeaders() != null) {
            contentType = getHttpHeaders().getContentType();
        }
        return contentType;
    }


    /**
     * Returns the etag value.
     *
     * @return the etag
     */
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


    /**
     * Returns the type value.
     *
     * @return the type
     */
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


    /**
     * Returns a {@link java.io.File} containing this object's data, if such a file has been provided.
     * Otherwise returns null.
     *
     * @return the dataInputFile
     */
    public final File getDataInputFile() {
        return this.dataInputFile;
    }


    /**
     * Sets the dataInputFile value.
     *
     * @param dataInputFile the dataInputFile to set
     */
    public final void setDataInputFile(final File dataInputFile) {
        this.dataInputFile = dataInputFile;
    }


    /**
     * Returns an {@link java.io.InputStream} containing this object's data,
     * or null if there is no data associated with this object.
     *
     * @return the dataInputStream
     * @throws IOException
     *             If an IO exception has occured.
     */
    public final InputStream getDataInputStream() throws IOException {
        if (this.dataInputStream == null) {
            if (this.dataInputFile != null) {
                this.dataInputStream = new FileInputStream(this.dataInputFile);
            } else if (this.dataInputString != null) {
                this.dataInputStream = new ByteArrayInputStream(this.dataInputString.getBytes("UTF-8"));
            }
        }
        return this.dataInputStream;
    }


    /**
     * Sets the {@link java.io.InputStream} containing the data content of this object.
     *
     * @param dataInputStream the dataInputStream to set
     */
    public final void setDataInputStream(final InputStream dataInputStream) {
        this.dataInputStream = dataInputStream;
    }


    /**
     * Return the {@link String} containing this object's data. If the object's data is contained in the
     * {@link InputStream}, then the data is read from the {@link InputStream}, returned as this {@link String} and the
     * {@link InputStream} is closed. If the object's data is contained in a {@link File}, then the file is read back
     * into the {@link String} .
     *
     * @return the dataInputString
     * @throws IOException
     *             If an IO exception has occured.
     */
    public final String getDataInputString() throws IOException {
        if (this.dataInputString == null) {
            if (this.dataInputStream != null) {
                this.dataInputString = MantaUtils.inputStreamToString(this.dataInputStream);
            } else if (this.dataInputFile != null) {
                this.dataInputString = MantaUtils.readFileToString(this.dataInputFile);
            }
        }
        return this.dataInputString;
    }


    /**
     * Sets the dataInputStrint value.
     *
     * @param dataInputString the dataInputString to set
     */
    public final void setDataInputString(final String dataInputString) {
        this.dataInputString = dataInputString;
    }


    /**
     * Returns the http headers.
     *
     * @return the httpHeaders
     */
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


    /**
     * This really just delegates to {@link com.google.api.client.http.HttpHeaders} get.
     *
     * @param fieldName the custom header to get from the Manta object.
     * @return the value of the header.
     */
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


    /**
     * @return whether this object is a Manta directory.
     */
    public final boolean isDirectory() {
        return MANTA_OBJECT_TYPE_DIRECTORY.equals(type);
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
        MantaObject that = (MantaObject)o;
        return Objects.equals(path, that.path)
                && Objects.equals(getContentLength(), that.getContentLength())
                && Objects.equals(getContentType(), that.getContentType())
                && Objects.equals(getEtag(), that.getEtag())
                && Objects.equals(getMtime(), that.getMtime())
                && Objects.equals(dataInputFile, that.dataInputFile)
                && Objects.equals(dataInputStream, that.dataInputStream)
                && Objects.equals(dataInputString, that.dataInputString)
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
                dataInputFile,
                dataInputStream,
                dataInputString,
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
        sb.append(", dataInputFile=").append(dataInputFile);
        sb.append(", dataInputStream=").append(dataInputStream);
        sb.append(", dataInputString='").append(dataInputString).append('\'');
        sb.append(", httpHeaders=").append(httpHeaders);
        sb.append(", directory=").append(isDirectory());
        sb.append('}');
        return sb.toString();
    }


}
