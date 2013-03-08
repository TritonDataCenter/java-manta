/**
 * Copyright (c) 2013, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

import org.apache.commons.io.FileUtils;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.util.Key;

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

        public static final String DIRECTORY_HEADER = "application/x-json-stream; type=directory";

        /**
         * JSON object metadata fields returned by {@link MantaClient}.listDir().
         */
        @Key("name")
        private String path_;

        @Key("size")
        private Long contentLength_;

        @Key("type")
        private String contentType_;

        @Key("etag")
        private String etag_;

        @Key("mtime")
        private String mtime_;

        /**
         * Other private members.
         */
        private File dataInputFile_;

        private InputStream dataInputStream_;

        private String dataInputString_;

        private HttpHeaders httpHeaders_;

        /**
         * Empty constructor for the JSON parser.
         */
        public MantaObject() {
        }

        /**
         * Creates a MantaObject
         * 
         * @param path
         *                The fully qualified path of the object in Manta. i.e. "/user/stor/path/to/some/file/or/dir".
         */
        public MantaObject(String path) {
                path_ = path;
                httpHeaders_ = new HttpHeaders();
        }

        /**
         * Creates a MantaObject
         * 
         * @param path
         *                The fully qualified path of the object in Manta. i.e. "/user/stor/path/to/some/file/or/dir".
         * @param headers
         *                Optional {@link HttpHeaders}. Use this to set any additional headers on the Manta object. For
         *                the full list of Manta headers see the <a href="http://apidocs.joyent.com/manta/manta/">Manta
         *                API</a>.
         */
        public MantaObject(String path, HttpHeaders headers) {
                path_ = path;
                httpHeaders_ = headers;
        }

        /* (non-Javadoc)
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object obj) {
                if (this == obj)
                        return true;
                if (obj == null)
                        return false;
                if (!(obj instanceof MantaObject))
                        return false;
                MantaObject other = (MantaObject) obj;
                if (contentLength_ == null) {
                        if (other.contentLength_ != null)
                                return false;
                } else if (!contentLength_.equals(other.contentLength_))
                        return false;
                if (contentType_ == null) {
                        if (other.contentType_ != null)
                                return false;
                } else if (!contentType_.equals(other.contentType_))
                        return false;
                if (dataInputFile_ == null) {
                        if (other.dataInputFile_ != null)
                                return false;
                } else if (!dataInputFile_.equals(other.dataInputFile_))
                        return false;
                if (dataInputStream_ == null) {
                        if (other.dataInputStream_ != null)
                                return false;
                } else if (!dataInputStream_.equals(other.dataInputStream_))
                        return false;
                if (dataInputString_ == null) {
                        if (other.dataInputString_ != null)
                                return false;
                } else if (!dataInputString_.equals(other.dataInputString_))
                        return false;
                if (etag_ == null) {
                        if (other.etag_ != null)
                                return false;
                } else if (!etag_.equals(other.etag_))
                        return false;
                if (httpHeaders_ == null) {
                        if (other.httpHeaders_ != null)
                                return false;
                } else if (!httpHeaders_.equals(other.httpHeaders_))
                        return false;
                if (mtime_ == null) {
                        if (other.mtime_ != null)
                                return false;
                } else if (!mtime_.equals(other.mtime_))
                        return false;
                if (path_ == null) {
                        if (other.path_ != null)
                                return false;
                } else if (!path_.equals(other.path_))
                        return false;
                return true;
        }

        /**
         * @return the size
         */
        public final Long getContentLength() {
                return contentLength_;
        }

        /**
         * @return the type
         */
        public final String getContentType() {
                return contentType_;
        }

        /**
         * Returns a {@link File} containing this object's data, if such a file has been provided. Otherwise returns
         * null.
         * 
         * @return the dataInputFile
         */
        public final File getDataInputFile() {
                return dataInputFile_;
        }

        /**
         * Returns an {@link InputStream} containing this object's data, or null if there is no data associated with
         * this object.
         * 
         * @return the dataInputStream
         * @throws IOException
         */
        public final InputStream getDataInputStream() throws IOException {
                if (dataInputStream_ == null) {
                        if (dataInputFile_ != null) {
                                dataInputStream_ = new FileInputStream(dataInputFile_);
                        } else if (dataInputString_ != null) {
                                dataInputStream_ = new ByteArrayInputStream(dataInputString_.getBytes("UTF-8"));
                        }
                }
                return dataInputStream_;
        }

        /**
         * Return the {@link String} containing this object's data. If the object's data is contained in the
         * {@link InputStream}, then the data is read from the {@link InputStream}, returned as this {@link String} and
         * the {@link InputStream} is closed. If the object's data is contained in a {@link File}, then the file is read
         * back into the {@link String} .
         * 
         * @return the dataInputString
         * @throws IOException
         */
        public final String getDataInputString() throws IOException {
                if (dataInputString_ == null) {
                        if (dataInputStream_ != null) {
                                dataInputString_ = MantaUtils.inputStreamToString(dataInputStream_);
                        } else if (dataInputFile_ != null) {
                                dataInputString_ = FileUtils.readFileToString(dataInputFile_);
                        }
                }
                return dataInputString_;
        }

        /**
         * @return the etag
         */
        public final String getEtag() {
                return etag_;
        }

        /**
         * This really just delegates to {@link HttpHeaders}.get.
         * 
         * @param fieldName
         *                the custom header to get from the Manta object.
         * @return the value of the header.
         */
        public final Object getHeader(String fieldName) {
                return httpHeaders_.get(fieldName);
        }

        /**
         * @return the httpHeaders
         */
        public final HttpHeaders getHttpHeaders() {
                return httpHeaders_;
        }

        /**
         * @return the mtime
         */
        public final String getMtime() {
                return mtime_;
        }

        /**
         * @return the path
         */
        public final String getPath() {
                return path_;
        }

        /* (non-Javadoc)
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
                final int prime = 31;
                int result = 1;
                result = prime * result + ((contentLength_ == null) ? 0 : contentLength_.hashCode());
                result = prime * result + ((contentType_ == null) ? 0 : contentType_.hashCode());
                result = prime * result + ((dataInputFile_ == null) ? 0 : dataInputFile_.hashCode());
                result = prime * result + ((dataInputStream_ == null) ? 0 : dataInputStream_.hashCode());
                result = prime * result + ((dataInputString_ == null) ? 0 : dataInputString_.hashCode());
                result = prime * result + ((etag_ == null) ? 0 : etag_.hashCode());
                result = prime * result + ((httpHeaders_ == null) ? 0 : httpHeaders_.hashCode());
                result = prime * result + ((mtime_ == null) ? 0 : mtime_.hashCode());
                result = prime * result + ((path_ == null) ? 0 : path_.hashCode());
                return result;
        }

        public final boolean isDirectory() {
                return contentType_.equals(DIRECTORY) || contentType_.equals(DIRECTORY_HEADER);
        }

        /**
         * @param dataInputFile
         *                the dataInputFile_ to set
         */
        public final void setDataInputFile(File dataInputFile) {
                this.dataInputFile_ = dataInputFile;
        }

        /**
         * Sets the {@link InputStream} containing the data content of this object.
         * 
         * @param dataInputStream
         *                the dataInputStream_ to set
         */
        public final void setDataInputStream(InputStream dataInputStream) {
                this.dataInputStream_ = dataInputStream;
        }

        /**
         * @param dataInputString
         *                the dataInputString to set
         */
        public final void setDataInputString(String dataInputString) {
                this.dataInputString_ = dataInputString;
        }

        /**
         * Sets custom headers on the Manta object. This really just delegates to setting the {@link HttpHeaders}
         * object. For the full list of Manta headers see the <a href="http://apidocs.joyent.com/manta/manta/">Manta
         * API</a>.
         * 
         * @param fieldName
         *                the field name.
         * @param value
         *                the field value.
         */
        public final void setHeader(String fieldName, Object value) {
                this.httpHeaders_.set(fieldName, value);
        }

        /**
         * Sets the {@link HttpHeaders} in this object. Note any previous headers will be lost. For the full list of
         * Manta headers see the <a href="http://apidocs.joyent.com/manta/manta/">Manta API</a>.
         * 
         * @param httpHeaders
         *                the httpHeaders_ to set
         */
        public final void setHttpHeaders(HttpHeaders httpHeaders) {
                this.httpHeaders_ = httpHeaders;
        }

        /**
         * @param path
         *                the path to set
         */
        public final void setPath(String path) {
                this.path_ = path;
        }

        /* (non-Javadoc)
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
                StringBuilder builder = new StringBuilder();
                builder.append("MantaObject [path_=").append(path_).append(", contentLength_=").append(contentLength_)
                                .append(", contentType_=").append(contentType_).append(", etag_=").append(etag_)
                                .append(", mtime_=").append(mtime_).append(", dataInputFile_=").append(dataInputFile_)
                                .append(", dataInputStream_=").append(dataInputStream_).append(", dataInputString_=")
                                .append(dataInputString_).append(", httpHeaders_=").append(httpHeaders_).append("]");
                return builder.toString();
        }

}
