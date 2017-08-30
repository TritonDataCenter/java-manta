/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
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
@SuppressWarnings("SameParameterValue")
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
                // Detect based on destination and then source
                // filename.  URLConnection uses a property list
                // bundled with the JVM and is expected to be
                // consistent on all platforms.  As implied by the
                // method name, the contents of the file are not
                // considered.
                URLConnection.guessContentTypeFromName(filename),
                URLConnection.guessContentTypeFromName(file.getName()),
                // Probe using the JVM default detection method.  The
                // detection methods vary across platforms and may
                // rely on /etc/mime.types or include native libraries
                // such as libgio or libreal.  The contents of the
                // file may be inspected.  This check is ordered last
                // both for cross-platform consistency.  See
                // https://github.com/joyent/java-manta/issues/276 for
                // further context.
                Files.probeContentType(file.toPath())
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
