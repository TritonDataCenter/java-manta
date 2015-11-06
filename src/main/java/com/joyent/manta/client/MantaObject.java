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

    private static final long serialVersionUID = -4129858089749197223L;

    /**
     * Metadata names used by Manta.
     */
    public static final String DIRECTORY = "directory";

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
     * The type value for this object.
     */
    @Key("type")
    private String contentType;

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
        return this.contentLength;
    }

    /**
     * Returns the content type value.
     *
     * @return the type
     */
    public final String getContentType() {
        return this.contentType;
    }


    /**
     * Returns the etag value.
     *
     * @return the etag
     */
    public final String getEtag() {
        return this.etag;
    }


    /**
     * Returns the mtime value.
     *
     * @return the mtime
     */
    public final String getMtime() {
        return this.mtime;
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
        return DIRECTORY.equals(contentType)
                || DIRECTORY_HEADER.equals(contentType);
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
                && Objects.equals(contentLength, that.contentLength)
                && Objects.equals(contentType, that.contentType)
                && Objects.equals(etag, that.etag)
                && Objects.equals(mtime, that.mtime)
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
                contentLength,
                contentType,
                etag,
                mtime,
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
        sb.append(", contentLength=").append(contentLength);
        sb.append(", contentType='").append(contentType).append('\'');
        sb.append(", etag='").append(etag).append('\'');
        sb.append(", mtime='").append(mtime).append('\'');
        sb.append(", dataInputFile=").append(dataInputFile);
        sb.append(", dataInputStream=").append(dataInputStream);
        sb.append(", dataInputString='").append(dataInputString).append('\'');
        sb.append(", httpHeaders=").append(httpHeaders);
        sb.append(", directory=").append(isDirectory());
        sb.append('}');
        return sb.toString();
    }

    
}
