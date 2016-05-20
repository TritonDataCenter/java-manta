package com.joyent.manta.client;

import java.io.Serializable;
import java.util.Date;

/**
 * Interface representing the contract that all result objects from
 * {@link MantaClient} must implement.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public interface MantaObject extends Serializable {
    /**
     * The type value for data objects within the manta service.
     */
    String MANTA_OBJECT_TYPE_OBJECT = "object";

    /**
     * The type value for directory objects within the manta service.
     */
    String MANTA_OBJECT_TYPE_DIRECTORY = "directory";

    /**
     * Manta directory header.
     */
    String DIRECTORY_HEADER = "application/x-json-stream; type=directory";

    /**
     * Returns the path value.
     *
     * @return the path
     */
    String getPath();

    /**
     * Returns the content length (size) value.
     *
     * @return content length (size)
     */
    Long getContentLength();

    /**
     * Returns the content type value.
     *
     * @return the type
     */
    String getContentType();

    /**
     * Returns the etag value.
     *
     * @return the etag
     */
    String getEtag();

    /**
     * Returns HTTP Computed-Md5 HTTP response as an array of bytes.
     * @return the computed md5 value as a byte array
     */
    byte[] getComputedMd5AsBytes();

    /**
     * Get the mtime value (last modified time) as a {@link java.util.Date} object.
     *
     * @return null if not available or unable to parse, otherwise last modified date in UTC
     */
    Date getLastModifiedTime();


    /**
     * Returns the mtime value.
     *
     * @return the mtime
     */
    String getMtime();


    /**
     * Returns the type value.
     *
     * @return the type
     */
    String getType();

    /**
     * Returns the http headers.
     *
     * @return the httpHeaders
     */
    MantaHttpHeaders getHttpHeaders();

    /**
     * This really just delegates to {@link MantaHttpHeaders} get.
     *
     * @param fieldName the custom header to get from the Manta object.
     * @return the value of the header.
     */
    Object getHeader(String fieldName);


    /**
     * Get a header in its {@link String} representation.
     *
     * @param fieldName header name
     * @return header as string
     */
    String getHeaderAsString(String fieldName);


    /**
     * Get the user-supplied metadata for this object.
     *
     * @return the user-supplied metadata
     */
    MantaMetadata getMetadata();


    /**
     * Unique request id made to Manta.
     *
     * @return UUID string representing a request id
     */
    String getRequestId();

    /**
     * @return whether this object is a Manta directory.
     */
    boolean isDirectory();
}
