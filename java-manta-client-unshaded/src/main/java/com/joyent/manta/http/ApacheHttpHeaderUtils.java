/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpMessage;

import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Utility methods for dealing with {@link Header}s in {@link HttpMessage}s like
 * {@link org.apache.http.client.methods.HttpUriRequest} or {@link org.apache.http.HttpResponse}.
 */
final class ApacheHttpHeaderUtils {

    private ApacheHttpHeaderUtils() {
    }

    /**
     * Extracts a single non-blank header value from the provided request or response, or null if the header wasn't
     * present.
     *
     * @param message the {@link org.apache.http.HttpRequest} or {@link org.apache.http.HttpResponse}
     * @param headerName the name of the request/repsonse header
     * @return the single header value if there was only one present, or null if it was missing
     * @throws HttpException When the header is present more than once, or is present but blank.
     */
    static String extractSingleHeaderValue(final HttpMessage message,
                                           final String headerName,
                                           final boolean required)
            throws HttpException {
        final Header[] headers = message.getHeaders(headerName);

        if (0 == headers.length) {
            if (required) {
                throw new HttpException(
                        String.format(
                                "Required [%s] header for resumable downloads missing",
                                headerName));
            }

            return null;
        }

        if (1 < headers.length) {
            throw new HttpException(
                    String.format(
                            "Resumable download not compatible with multi-valued [%s] header",
                            headerName));
        }

        // there is a single header
        final Header header = headers[0];

        if (header == null || isBlank(header.getValue())) {
            throw new HttpException(
                    String.format("Invalid %s header (blank or missing)", headerName));
        }

        return header.getValue();
    }
}
