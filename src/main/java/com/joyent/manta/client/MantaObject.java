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
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.util.Key;
import com.joyent.manta.exception.MantaObjectException;

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
        public static final String DURABILITY_LEVEL = "durability-level";

        /**
         * JSON object metadata fields returned by Manta.
         */
        @Key("name")
        private String path_;

        @Key("mtime")
        private String mtime_;

        @Key("type")
        private String contentType_;

        @Key("etag")
        private String etag_;

        @Key("size")
        private Long contentLength_;

        private String contentMd5_;
        private InputStream dataInputStream_;
        private File dataInputFile_;
        private String dataInputString_;
        private Integer durabilityLevel_;
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
        }

        /**
         * Creates a MantaObject
         * 
         * @param path
         *                The fully qualified path of the object in Manta. i.e. "/user/stor/path/to/some/file/or/dir".
         * @param headers
         *                Optional {@link HttpHeaders}.
         */
        public MantaObject(String path, HttpHeaders headers) throws MantaObjectException {
                path_ = path;
                httpHeaders_ = headers;
                mtime_ = headers.getLastModified();
                contentType_ = headers.getContentType();
                etag_ = headers.getETag();
                contentMd5_ = headers.getContentMD5();
                contentLength_ = headers.getContentLength();
                ArrayList durability = (ArrayList) headers.get(DURABILITY_LEVEL);
                int arraySize = durability.size();
                if (arraySize == 0) {
                    durabilityLevel_ = null;
                } else if (arraySize == 1) {
                    durabilityLevel_ = new Integer((String) durability.get(0));
                } else {
                    throw new MantaObjectException("More than one durability "
                            + "level is present in the HTTP headers");
                }
        }

        /*
         * (non-Javadoc)
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
                if (dataInputFile_ == null) {
                        if (other.dataInputFile_ != null)
                                return false;
                } else if (!dataInputFile_.equals(other.dataInputFile_))
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
                if (contentMd5_ == null) {
                        if (other.contentMd5_ != null)
                                return false;
                } else if (!contentMd5_.equals(other.contentMd5_))
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
                if (durabilityLevel_ == null) {
                        if (other.durabilityLevel_ != null)
                                return false;
                } else if (!durabilityLevel_.equals(other.durabilityLevel_))
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
         * @return the md5
         */
        public final String getContentMd5() {
                return contentMd5_;
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
         * @return the durability level
         */
        public final Integer getDurabilityLevel() {
                return durabilityLevel_;
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

        /*
         * (non-Javadoc)
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
                final int prime = 31;
                int result = 1;
                result = prime * result + ((dataInputFile_ == null) ? 0 : dataInputFile_.hashCode());
                result = prime * result + ((dataInputString_ == null) ? 0 : dataInputString_.hashCode());
                result = prime * result + ((etag_ == null) ? 0 : etag_.hashCode());
                result = prime * result + ((httpHeaders_ == null) ? 0 : httpHeaders_.hashCode());
                result = prime * result + ((contentMd5_ == null) ? 0 : contentMd5_.hashCode());
                result = prime * result + ((mtime_ == null) ? 0 : mtime_.hashCode());
                result = prime * result + ((path_ == null) ? 0 : path_.hashCode());
                result = prime * result + ((contentLength_ == null) ? 0 : contentLength_.hashCode());
                result = prime * result + ((contentType_ == null) ? 0 : contentType_.hashCode());
                result = prime * result + ((durabilityLevel_ == null) ? 0 : durabilityLevel_.hashCode());
                return result;
        }

        public final boolean isDirectory() {
                return contentType_.equals(DIRECTORY) || contentType_.equals(DIRECTORY_HEADER);
        }

        /**
         * @param contentMd5
         *                the md5 to set
         */
        public final void setContentMd5(String contentMd5) {
                this.contentMd5_ = contentMd5;
                if (httpHeaders_ == null) {
                        httpHeaders_ = new HttpHeaders();
                }
                httpHeaders_.setContentMD5(contentMd5);
        }

        /**
         * @param size_
         *                the size_ to set
         */
        public final void setContentSize(Long contentSize) {
                this.contentLength_ = contentSize;
                if (httpHeaders_ == null) {
                        httpHeaders_ = new HttpHeaders();
                }
                httpHeaders_.setContentLength(contentSize);
        }

        /**
         * @param contentType
         *                the type to set
         */
        public final void setContentType(String contentType) {
                this.contentType_ = contentType;
                if (httpHeaders_ == null) {
                        httpHeaders_ = new HttpHeaders();
                }
                httpHeaders_.setContentType(contentType);
        }

        /**
         * @param durabilityLevel the durability level to set
         */
        public final void setDurabilityLevel(int durabilityLevel) {
                this.durabilityLevel_ = durabilityLevel;
                if (httpHeaders_ == null) {
                        httpHeaders_ = new HttpHeaders();
                }
                httpHeaders_.set(DURABILITY_LEVEL, durabilityLevel);
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
         * Sets the {@link HttpHeaders} in this object. Note any previous headers will be lost.
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

        /*
         * (non-Javadoc)
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
                StringBuilder builder = new StringBuilder();
                builder.append("MantaObject [path_=").append(path_).append(", mtime_=").append(mtime_)
                                .append(", type_=").append(contentType_).append(", etag_=").append(etag_)
                                .append(", size_=").append(contentLength_).append(", md5_=").append(contentMd5_)
                                .append(", dataInputStream_=").append(dataInputStream_).append(", dataInputFile_=")
                                .append(dataInputFile_).append(", dataInputString_=").append(dataInputString_)
                                .append(", httpHeaders_=").append(httpHeaders_).append("]").append(", durabilityLevel_=")
                                .append(durabilityLevel_).append("]");
                return builder.toString();
        }

}
