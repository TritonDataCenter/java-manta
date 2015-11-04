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

    private static final long serialVersionUID = 1465638185667442505L;

    /**
     * Metadata names used by Manta.
     */
    public static final String DIRECTORY = "directory";

    /**
     * Manta directory header.
     */
    public static final String DIRECTORY_HEADER = "application/x-json-stream; type=directory";

    /**
     * JSON object metadata fields returned by {@link MantaClient}.listDir().
     */
    @Key("name")
    private String path;

    @Key("size")
    private Long contentLength;

    @Key("type")
    private String contentType;

    @Key("etag")
    private String etag;

    @Key("mtime")
    private String mtime;

    /**
     * Other private members.
     */
    private File dataInputFile;

    private InputStream dataInputStream;

    private String dataInputString;

    private HttpHeaders httpHeaders;

    /**
     * Empty constructor for the JSON parser.
     */
    public MantaObject() {
    }

    /**
     * Creates a MantaObject.
     *
     * @param path
     *            The fully qualified path of the object in Manta. i.e. "/user/stor/path/to/some/file/or/dir".
     */
    public MantaObject(final String path) {
        this.path = path;
        this.httpHeaders = new HttpHeaders();
    }

    /**
     * Creates a MantaObject.
     *
     * @param path
     *            The fully qualified path of the object in Manta. i.e. "/user/stor/path/to/some/file/or/dir".
     * @param headers
     *            Optional {@link HttpHeaders}. Use this to set any additional headers on the Manta object. For the full
     *            list of Manta headers see the <a href="http://apidocs.joyent.com/manta/manta/">Manta API</a>.
     */
    public MantaObject(final String path, final HttpHeaders headers) {
        this.path = path;
        this.httpHeaders = headers;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public final boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof MantaObject)) {
            return false;
        }
        final MantaObject other = (MantaObject) obj;
        if (this.contentLength == null) {
            if (other.contentLength != null) {
                return false;
            }
        } else if (!this.contentLength.equals(other.contentLength)) {
            return false;
        }
        if (this.contentType == null) {
            if (other.contentType != null) {
                return false;
            }
        } else if (!this.contentType.equals(other.contentType)) {
            return false;
        }
        if (this.dataInputFile == null) {
            if (other.dataInputFile != null) {
                return false;
            }
        } else if (!this.dataInputFile.equals(other.dataInputFile)) {
            return false;
        }
        if (this.dataInputStream == null) {
            if (other.dataInputStream != null) {
                return false;
            }
        } else if (!this.dataInputStream.equals(other.dataInputStream)) {
            return false;
        }
        if (this.dataInputString == null) {
            if (other.dataInputString != null) {
                return false;
            }
        } else if (!this.dataInputString.equals(other.dataInputString)) {
            return false;
        }
        if (this.etag == null) {
            if (other.etag != null) {
                return false;
            }
        } else if (!this.etag.equals(other.etag)) {
            return false;
        }
        if (this.httpHeaders == null) {
            if (other.httpHeaders != null) {
                return false;
            }
        } else if (!this.httpHeaders.equals(other.httpHeaders)) {
            return false;
        }
        if (this.mtime == null) {
            if (other.mtime != null) {
                return false;
            }
        } else if (!this.mtime.equals(other.mtime)) {
            return false;
        }
        if (this.path == null) {
            if (other.path != null) {
                return false;
            }
        } else if (!this.path.equals(other.path)) {
            return false;
        }
        return true;
    }

    /**
     * @return the size
     */
    public final Long getContentLength() {
        return this.contentLength;
    }

    /**
     * @return the type
     */
    public final String getContentType() {
        return this.contentType;
    }

    /**
     * Returns a {@link File} containing this object's data, if such a file has been provided. Otherwise returns null.
     *
     * @return the dataInputFile
     */
    public final File getDataInputFile() {
        return this.dataInputFile;
    }

    /**
     * Returns an {@link InputStream} containing this object's data, or null if there is no data associated with this
     * object.
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
     * @return the etag
     */
    public final String getEtag() {
        return this.etag;
    }

    /**
     * This really just delegates to {@link HttpHeaders}.get.
     *
     * @param fieldName
     *            the custom header to get from the Manta object.
     * @return the value of the header.
     */
    public final Object getHeader(final String fieldName) {
        return this.httpHeaders.get(fieldName);
    }

    /**
     * @return the httpHeaders
     */
    public final HttpHeaders getHttpHeaders() {
        return this.httpHeaders;
    }

    /**
     * @return the mtime
     */
    public final String getMtime() {
        return this.mtime;
    }

    /**
     * @return the path
     */
    public final String getPath() {
        return this.path;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public final int hashCode() {
        final int prime = 31;
        int result = 1;
        result = (prime * result) + ((this.contentLength == null) ? 0 : this.contentLength.hashCode());
        result = (prime * result) + ((this.contentType == null) ? 0 : this.contentType.hashCode());
        result = (prime * result) + ((this.dataInputFile == null) ? 0 : this.dataInputFile.hashCode());
        result = (prime * result) + ((this.dataInputStream == null) ? 0 : this.dataInputStream.hashCode());
        result = (prime * result) + ((this.dataInputString == null) ? 0 : this.dataInputString.hashCode());
        result = (prime * result) + ((this.etag == null) ? 0 : this.etag.hashCode());
        result = (prime * result) + ((this.httpHeaders == null) ? 0 : this.httpHeaders.hashCode());
        result = (prime * result) + ((this.mtime == null) ? 0 : this.mtime.hashCode());
        result = (prime * result) + ((this.path == null) ? 0 : this.path.hashCode());
        return result;
    }



    /**
     * @return whether this object is a Manta directory.
     */
    public final boolean isDirectory() {
        return this.contentType.equals(DIRECTORY) || this.contentType.equals(DIRECTORY_HEADER);
    }

    /**
     * @param dataInputFile
     *            the dataInputFile to set
     */
    public final void setDataInputFile(final File dataInputFile) {
        this.dataInputFile = dataInputFile;
    }

    /**
     * Sets the {@link InputStream} containing the data content of this object.
     *
     * @param dataInputStream
     *            the dataInputStream to set
     */
    public final void setDataInputStream(final InputStream dataInputStream) {
        this.dataInputStream = dataInputStream;
    }

    /**
     * @param dataInputString
     *            the dataInputString to set
     */
    public final void setDataInputString(final String dataInputString) {
        this.dataInputString = dataInputString;
    }

    /**
     * Sets custom headers on the Manta object. This really just delegates to setting the {@link HttpHeaders} object.
     * For the full list of Manta headers see the <a href="http://apidocs.joyent.com/manta/manta/">Manta API</a>.
     *
     * @param fieldName
     *            the field name.
     * @param value
     *            the field value.
     */
    public final void setHeader(final String fieldName, final Object value) {
        this.httpHeaders.set(fieldName, value);
    }

    /**
     * Sets the {@link HttpHeaders} in this object. Note any previous headers will be lost. For the full list of Manta
     * headers see the <a href="http://apidocs.joyent.com/manta/manta/">Manta API</a>.
     *
     * @param httpHeaders
     *            the httpHeaders to set
     */
    public final void setHttpHeaders(final HttpHeaders httpHeaders) {
        this.httpHeaders = httpHeaders;
    }

    /**
     * @param path
     *            the path to set
     */
    public final void setPath(final String path) {
        this.path = path;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public final String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("MantaObject [path=").append(this.path).append(", contentLength=")
        .append(this.contentLength).append(", contentType=").append(this.contentType).append(", etag=")
        .append(this.etag).append(", mtime=").append(this.mtime).append(", dataInputFile=")
        .append(this.dataInputFile).append(", dataInputStream=").append(this.dataInputStream)
        .append(", dataInputString=").append(this.dataInputString).append(", httpHeaders=")
        .append(this.httpHeaders).append("]");
        return builder.toString();
    }
}
