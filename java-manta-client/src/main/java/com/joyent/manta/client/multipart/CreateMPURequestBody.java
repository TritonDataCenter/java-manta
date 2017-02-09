/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;

import java.util.Map;
import java.util.Set;

/**
 * Value object used to represent the JSON payload sent to Manta when creating
 * a new multipart upload.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
class CreateMPURequestBody {
    /**
     * Path to the object on Manta.
     */
    private final String objectPath;

    /**
     * Headers associated with object.
     */
    private final Map<String, Object> headers = new CaseInsensitiveMap<>();

    /**
     * <p>Creates a new instance with header fields populated from header object
     * and metadata object.</p>
     *
     * <p>The only HTTP headers supported are:</p>
     * <ol>
     *     <li><code>durability-level</code></li>
     *     <li><code>content-length</code></li>
     *     <li><code>content-md5</code></li>
     * </ol>
     *
     * @param objectPath path to the object on Manta
     * @param metadata metadata associated with object
     * @param mantaHttpHeaders HTTP headers associated with object
     */
    CreateMPURequestBody(final String objectPath,
                         final MantaMetadata metadata,
                         final MantaHttpHeaders mantaHttpHeaders) {
        this.objectPath = objectPath;

        if (mantaHttpHeaders != null) {
            importHeaders(mantaHttpHeaders);
        }

        if (metadata != null) {
            importMetadata(metadata);
        }
    }

    /**
     * Imports the passed HTTP headers object into the JSON headers object.
     *
     * @param mantaHttpHeaders HTTP headers associated with object
     */
    private void importHeaders(final MantaHttpHeaders mantaHttpHeaders) {
        String contentType = mantaHttpHeaders.getContentType();
        if (!StringUtils.isBlank(contentType)) {
            this.headers.put(HttpHeaders.CONTENT_TYPE, contentType);
        }
        Integer durabilityLevel = mantaHttpHeaders.getDurabilityLevel();
        if (durabilityLevel != null) {
            this.headers.put(MantaHttpHeaders.HTTP_DURABILITY_LEVEL, durabilityLevel);
        }
        Long contentLength = mantaHttpHeaders.getContentLength();
        if (contentLength != null) {
            this.headers.put(HttpHeaders.CONTENT_LENGTH, contentLength);
        }
        String contentMd5 = mantaHttpHeaders.getContentMD5();
        if (!StringUtils.isBlank(contentMd5)) {
            this.headers.put(HttpHeaders.CONTENT_MD5, contentMd5);
        }
        /* The roles header isn't supported in the initial MPU release, but it
         * may be in the future, so we are adding it because the server will
         * throw it away if it can't user it. */
        String roles = mantaHttpHeaders.getFirstHeaderStringValue(MantaHttpHeaders.HTTP_ROLE_TAG);
        if (!StringUtils.isBlank(roles)) {
            this.headers.put(MantaHttpHeaders.HTTP_ROLE_TAG, roles);
        }
    }

    /**
     * Imports the passed Manta metadata object into the JSON headers object.
     *
     * @param metadata metadata associated with object
     */
    private void importMetadata(final MantaMetadata metadata) {
        Set<Map.Entry<String, String>> entrySet = metadata.entrySet();

        for (Map.Entry<String, String> next : entrySet) {
            if (next.getKey().startsWith(MantaMetadata.METADATA_PREFIX)) {
                headers.put(next.getKey(), next.getValue());
            }
        }
    }

    @SuppressWarnings("unused")
    public String getObjectPath() {
        return objectPath;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, Object> getHeaders() {
        return headers;
    }
}
