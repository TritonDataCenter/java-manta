/**
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.http;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.http.entity.ContentType;

import java.io.File;
import java.io.IOException;
import java.net.URLConnection;
import java.nio.file.Files;

/**
 * Utility class that provides methods for looking up the HTTP content-type
 * of a given file or stream.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public final class ContentTypeLookup {
    /**
     * This is a utility class, so there is no need to instantiate it.
     */
    private ContentTypeLookup() {
    }

    /**
     * Finds the content type set in {@link MantaHttpHeaders} and returns that if it
     * is not null. Otherwise, it will return the specified default content type.
     *
     * @param headers headers to parse for content type
     * @param defaultContentType content type to default to
     * @return content type object
     */
    public static ContentType findOrDefaultContentType(final MantaHttpHeaders headers,
                                                       final ContentType defaultContentType) {
        if (headers == null || headers.getContentType() == null) {
            return defaultContentType;
        } else {
            return ContentType.parse(headers.getContentType());
        }
    }

    /**
     * Finds the content type set in {@link MantaHttpHeaders} and returns that if it
     * is not null. Otherwise, it will return the specified default content type.
     *
     * @param headers headers to parse for content type
     * @param filename path to the destination file
     * @param file file that is being probed for content type
     * @param defaultContentType content type to default to
     * @return content type object
     * @throws IOException thrown when we can't access the file being analyzed
     */
    public static ContentType findOrDefaultContentType(final MantaHttpHeaders headers,
                                                       final String filename,
                                                       final File file,
                                                       final ContentType defaultContentType)
            throws IOException {
        final String headerContentType;

        if (headers != null) {
            headerContentType = headers.getContentType();
        } else {
            headerContentType = null;
        }

        String type =  ObjectUtils.firstNonNull(
                // Use explicitly set headers if available
                headerContentType,
                // Probe using the JVM default detection method
                Files.probeContentType(file.toPath()),
                // Detect based on destination filename
                URLConnection.guessContentTypeFromName(filename),
                // Detect based on source filename
                URLConnection.guessContentTypeFromName(file.getName())
        );

        if (type == null) {
            return defaultContentType;
        }

        return ContentType.parse(type);
    }

    /**
     * Finds the content type set in {@link MantaHttpHeaders} and returns that if it
     * is not null. Otherwise, it will return the specified default content type.
     *
     * @param headers headers to parse for content type
     * @param filename filename that is being probed for content type
     * @param defaultContentType content type to default to
     * @return content type object
     */
    public static ContentType findOrDefaultContentType(final MantaHttpHeaders headers,
                                                       final String filename,
                                                       final ContentType defaultContentType) {
        final String headerContentType;

        if (headers != null) {
            headerContentType = headers.getContentType();
        } else {
            headerContentType = null;
        }

        String type = ObjectUtils.firstNonNull(
                // Use explicitly set headers if available
                headerContentType,
                // Detect based on filename
                URLConnection.guessContentTypeFromName(filename)
        );

        if (type == null) {
            return defaultContentType;
        }

        return ContentType.parse(type);
    }
}
