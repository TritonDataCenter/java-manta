/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.joyent.manta.exception.ResumableDownloadIncompatibleRequestException;
import com.joyent.manta.exception.ResumableDownloadUnexpectedResponseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpMessage;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;

import java.util.Arrays;

import static com.joyent.manta.http.HttpRange.parseContentRange;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.http.HttpHeaders.CONTENT_LENGTH;
import static org.apache.http.HttpHeaders.CONTENT_RANGE;
import static org.apache.http.HttpHeaders.ETAG;
import static org.apache.http.HttpHeaders.IF_MATCH;
import static org.apache.http.HttpHeaders.RANGE;

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

    /**
     * Checks that a request has headers which are compatible. This means that the request either:
     * <ul>
     * <li>has no If-Match header and no ETag header</li>
     * <li>has a single-valued If-Match header</li>
     * <li>has a single Range header specifying a single byte range (i.e. no multipart ranges)</li>
     * <li>satisfies both #2 and #3</li>
     * </ul>
     *
     * @param request the request being checked for compatibility
     * @return the ETag and range hints to be validated against the initial response
     * @throws ResumableDownloadIncompatibleRequestException when the request cannot be resumed
     */
    static Pair<String, HttpRange.Request> extractDownloadRequestFingerprint(final HttpGet request)
            throws ResumableDownloadIncompatibleRequestException {
        String ifMatch = null;
        HttpRange.Request range = null;

        Exception ifMatchEx = null;
        Exception rangeEx = null;

        try {
            ifMatch = extractSingleHeaderValue(request, IF_MATCH, false);
        } catch (final HttpException e) {
            ifMatchEx = e;
        }

        try {
            final String rawRequestRange = extractSingleHeaderValue(request, RANGE, false);
            if (rawRequestRange != null) {
                range = HttpRange.parseRequestRange(rawRequestRange);
            }
        } catch (final HttpException e) {
            rangeEx = e;
        }

        if (ifMatchEx != null && rangeEx != null) {
            throw new ResumableDownloadIncompatibleRequestException(
                    String.format(
                            "Incompatible Range and If-Match request headers for resuming download:%n%s%n%s",
                            rangeEx.getMessage(),
                            ifMatchEx.getMessage()));
        } else if (ifMatchEx != null) {
            throw new ResumableDownloadIncompatibleRequestException(ifMatchEx);
        } else if (rangeEx != null) {
            throw new ResumableDownloadIncompatibleRequestException(rangeEx);
        }

        return ImmutablePair.of(ifMatch, range);
    }

    /**
     * In order to be sure we're continuing to download the same object we need to extract the {@code ETag} and {@code
     * Content-Range} headers from the response. Either header missing is an error. Additionally, when the {@code
     * Content-Range} header is present the specified range should be equal to the response's {@code Content-Length}.
     *
     * @param response the response to check for headers
     * @return the request headers we're concerned with validating
     * @throws ResumableDownloadUnexpectedResponseException when the headers are malformed, unparseable, or the {@code
     * Content-Range} and {@code Content-Length} are mismatched
     */
    static Pair<String, HttpRange.Response> extractDownloadResponseFingerprint(final HttpResponse response)
            throws ResumableDownloadUnexpectedResponseException {

        final String etag;
        try {
            etag = extractSingleHeaderValue(response, ETAG, true);
        } catch (final HttpException e) {
            throw new ResumableDownloadUnexpectedResponseException(e);
        }

        final long contentLength;
        try {
            final String rawContentLength = extractSingleHeaderValue(response, CONTENT_LENGTH, true);
            contentLength = Long.parseUnsignedLong(rawContentLength);
        } catch (final HttpException e) {
            throw new ResumableDownloadUnexpectedResponseException(e);
        } catch (final NumberFormatException e) {
            throw new ResumableDownloadUnexpectedResponseException(
                    String.format(
                            "Failed to parse Content-Length response, matching headers: %s",
                            Arrays.deepToString(response.getHeaders(CONTENT_LENGTH))));
        }

        final String rawContentRange;
        try {
            rawContentRange = extractSingleHeaderValue(response, CONTENT_RANGE, false);
        } catch (final HttpException e) {
            throw new ResumableDownloadUnexpectedResponseException(e);
        }

        if (StringUtils.isBlank(rawContentRange)) {
            // the entire object is being requested
            return new ImmutablePair<>(etag, new HttpRange.Response(0, contentLength - 1, contentLength));
        }

        final HttpRange.Response contentRange;
        try {
            contentRange = parseContentRange(rawContentRange);
        } catch (final HttpException e) {
            throw new ResumableDownloadUnexpectedResponseException(
                    "Unable to parse Content-Range header while analyzing download response",
                    e);
        }

        // Manta follows the spec and sends the Content-Length of the range, which we should validateResponseWithMarker
        if (contentRange.contentLength() != contentLength) {
            throw new ResumableDownloadUnexpectedResponseException(
                    String.format(
                            "Invalid Content-Length in range response: expected [%d], got [%d]",
                            contentRange.contentLength(),
                            contentLength));
        }

        return new ImmutablePair<>(etag, contentRange);
    }


}
