/*
 * Copyright (c) 2019, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.joyent.manta.domain.ObjectType;
import com.joyent.manta.exception.MantaIOException;
import com.joyent.manta.exception.MantaResourceCloseException;
import com.joyent.manta.exception.MantaUnexpectedObjectTypeException;
import com.joyent.manta.http.HttpHelper;
import com.joyent.manta.http.MantaHttpHeaders;
import io.mikael.urlbuilder.util.Encoder;
import org.apache.commons.lang3.Validate;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.joyent.manta.client.MantaObjectResponse.DIRECTORY_RESPONSE_CONTENT_TYPE;
import static com.joyent.manta.util.MantaUtils.formatPath;
import static org.apache.commons.lang3.StringUtils.startsWith;

/**
 * <p>Class that wraps the paging of buckets listing in Manta to a single
 * continuous iterator.</p>
 *
 * <p><a href="https://apidocs.joyent.com/manta/api.html#ListBuckets">
 * Listing Manta Buckets</a> is done by getting pages of data with N number
 * of records per page. Each page of data is requested using a limit (number
 * of records) and a marker (the last seen item in the list). This class
 * automates that process and abstracts out the details of the paging process.</p>
 *
 * @author <a href="https://github.com/nairashwin952013">Ashwin A Nair</a>
 */
@SuppressWarnings({"Duplicates"})
public class MantaBucketListingIterator implements Iterator<Map<String, Object>>,
        AutoCloseable  {
    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(MantaBucketListingIterator.class);

    /**
     * Shared url encoder instance.
     */
    private static final Encoder UTF8_URL_ENCODER = new Encoder(StandardCharsets.UTF_8);

    /**
     * Maximum number of results to return for a bucket listing.
     */
    static final int MAX_RESULTS = 1024;

    /**
     * Size of result set requested against the Manta API (2-1024).
     */
    private final int limit;

    /**
     * Prefix filter that helps in optimizing a buckets listing.
     */
    private volatile String prefix = null;

    /**
     * Filter to group names with a common prefix ending in its first occurrence.
     */
    private Character delimiter = null;

    /**
     * Path to bucket in which we will iterate through its contents.
     */
    private final String path;

    /**
     * HTTP request helper class.
     */
    private final HttpHelper httpHelper;

    /**
     * The total number of lines that we have iterated through.
     */
    private final AtomicLong lines = new AtomicLong(0);

    /**
     * The next line of data that we haven't iterated to yet.
     */
    private final AtomicReference<String> nextLine = new AtomicReference<>();

    /**
     * Flag indicated if we have finished and there is nothing left to iterate.
     */
    private final AtomicBoolean finished = new AtomicBoolean(false);

    /**
     * Jackson JSON parsing instance.
     */
    private final ObjectMapper mapper = MantaObjectMapper.INSTANCE;

    /**
     * The marker we use to request against the Manta API.
     */
    private volatile String marker;

    /**
     * The current {@link BufferedReader} instance that wraps the HTTP response
     * {@link java.io.InputStream} from our most recent request to the API.
     */
    private volatile BufferedReader br;

    /**
     * The most recent response object from the page of data that we are currently
     * parsing.
     */
    private volatile CloseableHttpResponse currentResponse;

    /**
     * Create a new instance of a bucket list iterator.
     *
     * @param path path to bucket in which we will iterate through its contents
     * @param httpHelper HTTP request helper class
     * @param prefix prefix filter that helps in optimizing a buckets listing
     * @param limit size of result set requested against the Manta API (2-1024)
     * @param delimiter filter to group names with a common prefix ending in its first occurrence
     */
    public MantaBucketListingIterator(final String path,
                                      final HttpHelper httpHelper,
                                      final String prefix,
                                      final Character delimiter,
                                      final int limit) {
        Validate.notBlank(path, "Path must not be blank");
        Validate.notNull(httpHelper, "HTTP help must not be null");
        Validate.notNull(prefix, "Prefix parameter must not be null");
        Validate.notNull(delimiter, "Delimiter parameter must not be null");

        this.path = path;
        this.httpHelper = httpHelper;

        if (!(limit >= 2 && limit <= MAX_RESULTS)) {
            throw new IllegalArgumentException("Paging size must be greater than "
                    + "1 and less than or equal to 1024");
        }

        this.limit = limit;
        this.prefix = prefix;
        this.delimiter = delimiter;
    }

    /**
     * Create a new instance of a bucket list iterator.
     *
     * @param path path to bucket in which we will iterate through its contents
     * @param httpHelper HTTP request helper class
     * @param prefix prefix filter that helps in optimizing a buckets listing
     * @param limit size of result set requested against the Manta API (2-1024).
     */
    public MantaBucketListingIterator(final String path,
                                      final HttpHelper httpHelper,
                                      final String prefix,
                                      final int limit) {
        Validate.notBlank(path, "Path must not be blank");
        Validate.notNull(httpHelper, "HTTP help must not be null");
        Validate.notNull(prefix, "Prefix parameter must not be null");

        this.path = path;
        this.httpHelper = httpHelper;

        if (!(limit >= 2 && limit <= MAX_RESULTS)) {
            throw new IllegalArgumentException("Paging size must be greater than "
                    + "1 and less than or equal to 1024");
        }

        this.limit = limit;
        this.prefix = prefix;
    }

    /**
     * Create a new instance of a bucket list iterator.
     *
     * @param path path to bucket in which we will iterate through its contents
     * @param httpHelper HTTP request helper class
     * @param prefix prefix filter that helps in optimizing a buckets listing
     * @param marker marker we use to request against the Manta API.
     * @param limit size of result set requested against the Manta API (2-1024).
     */
    public MantaBucketListingIterator(final String path,
                                      final HttpHelper httpHelper,
                                      final String prefix,
                                      final String marker,
                                      final int limit) {
        Validate.notBlank(path, "Path must not be blank");
        Validate.notNull(httpHelper, "HTTP help must not be null");

        this.path = path;
        this.httpHelper = httpHelper;

        if (!(limit >= 2 && limit <= MAX_RESULTS)) {
            throw new IllegalArgumentException("Paging size must be greater than "
                    + "1 and less than or equal to 1024");
        }

        this.marker = marker;
        this.limit = limit;
        this.prefix = prefix;
    }

    /**
     * Create a new instance of a bucket list iterator.
     *
     * @param path path to bucket in which we will iterate through its contents
     * @param httpHelper HTTP request helper class
     * @param delimiter filter to group names with a common prefix ending in its first occurrence
     * @param limit size of result set requested against the Manta API (2-1024).
     */
    public MantaBucketListingIterator(final String path,
                                      final HttpHelper httpHelper,
                                      final Character delimiter,
                                      final int limit) {
        Validate.notBlank(path, "Path must not be blank");
        Validate.notNull(httpHelper, "HTTP help must not be null");
        Validate.notNull(delimiter, "Delimiter parameter must not be null");

        this.path = path;
        this.httpHelper = httpHelper;

        if (!(limit >= 2 && limit <= MAX_RESULTS)) {
            throw new IllegalArgumentException("Paging size must be greater than "
                    + "1 and less than or equal to 1024");
        }

        this.limit = limit;
        this.delimiter = delimiter;
    }

    /**
     * Create a new instance of a bucket list iterator.
     *
     * @param path path to bucket in which we will iterate through its contents
     * @param httpHelper HTTP request helper class
     * @param limit size of result set requested against the Manta API (2-1024).
     */
    public MantaBucketListingIterator(final String path,
                                      final HttpHelper httpHelper,
                                      final int limit) {
        Validate.notBlank(path, "Path must not be blank");
        Validate.notNull(httpHelper, "HTTP help must not be null");

        this.path = path;
        this.httpHelper = httpHelper;

        if (!(limit >= 2 && limit <= MAX_RESULTS)) {
            throw new IllegalArgumentException("Paging size must be greater than "
                    + "1 and less than or equal to 1024");
        }

        this.limit = limit;
    }

    /**
     * Chooses the next reader by opening a HTTP connection to get the next
     * page of input from the Manta API. If there isn't another page of data
     * available, we mark ourselves as finished.
     *
     * @throws IOException thrown when we can't successfully open an HTTP connection
     */
    private synchronized void selectReader() throws IOException {
        if (marker == null) {
            String query = queryGenerator();
             final HttpGet request = httpHelper.getRequestFactory().get(formatPath(path) + query);

            try {
                if (currentResponse != null) {
                    currentResponse.close();
                }
            } catch (IOException e) {
                MantaIOException mio = new MantaIOException(e);
                HttpHelper.annotateContextedException(mio, request, currentResponse);
                throw mio;
            }

            currentResponse = httpHelper.executeRequest(request, null);
            HttpEntity entity = currentResponse.getEntity();

            /* See description */
            validateEntityForBuckets(entity);

            InputStream contentStream = entity.getContent();
            Objects.requireNonNull(contentStream, "A bucket listing without "
                    + "content is not valid. Content is null.");

            Reader streamReader = new InputStreamReader(entity.getContent(),
                    StandardCharsets.UTF_8.name());
            br = new BufferedReader(streamReader);
        } else {
            String query = queryGenerator();
            final HttpGet request = httpHelper.getRequestFactory().get(formatPath(path) + query);

            closeResources();

            currentResponse = httpHelper.executeRequest(request, null);
            HttpEntity entity = currentResponse.getEntity();
            Reader streamReader = new InputStreamReader(entity.getContent(),
                    StandardCharsets.UTF_8.name());
            br = new BufferedReader(streamReader);
        }

        nextLine.set(br.readLine());
        lines.incrementAndGet();


        /* If we have an empty bucket with Next-Marker header as null, we mark as done right here
         * so that no other methods have additional work left. */
        Header nextMarkerHeader = currentResponse.getFirstHeader(MantaHttpHeaders.NEXT_MARKER);
        if (nextLine.get() == null && nextMarkerHeader == null) {
            finished.set(true);
            return;
        } else if (nextMarkerHeader == null) {
             LOG.info("Next-Marker value parsed is null for buckets listing");
        }

        // We are done if the first read is a null
        finished.set(nextLine.get() == null);
    }

    @Override
    public boolean hasNext() {
        if (!finished.get() && nextLine.get() == null) {
            try {
                selectReader();
                return !finished.get();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            return !finished.get();
        }
    }

    @Override
    public synchronized Map<String, Object> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        try {
            String line = nextLine.getAndSet(br.readLine());
            lines.incrementAndGet();

            if (line == null) {
                selectReader();

                if (finished.get()) {
                    throw new NoSuchElementException();
                }

                line = nextLine.getAndSet(br.readLine());
            }

            final Map<String, Object> lookup = mapper.readValue(line,
                    new TypeReference<Map<String, Object>>() { });
            final String name = Objects.toString(lookup.get("name"));

            Validate.notNull(name, "Name must not be null in JSON input");

            /* Explicitly set the path of the object here so that we don't need
             * to create a new instance of MantaObjectConversionFunction per
             * object being read. */
            lookup.put("path", path);

            this.marker = name;

            return lookup;

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        finished.set(true);

        closeResources();
    }

    /* ======================================================================
     * Class Helper Methods
     * ====================================================================== */

    /**
     * Closes dependent resources. If there is a problem closing the resources
     * an exception is logged but not thrown.
     */
    private void closeResources() {
        try {
            if (br != null) {
                br.close();
            }
        } catch (IOException e) {
            MantaIOException mio = new MantaResourceCloseException(e);
            HttpHelper.annotateContextedException(mio, null, currentResponse);
            LOG.error("Unable to close BufferedReader", mio);
        }

        try {
            if (currentResponse != null) {
                currentResponse.close();
            }
        } catch (IOException e) {
            MantaIOException mio = new MantaResourceCloseException(e);
            HttpHelper.annotateContextedException(mio, null, currentResponse);
            LOG.error("Unable to close HTTP response object", mio);
        }
    }

    /**
     * Validates whether the given listing operation (GET API call) has been
     * made to a valid buckets path and not to an unsupported directory.
     *
     * @param entity Apache HTTP Client content entity object
     * @throws MantaUnexpectedObjectTypeException Iterator applied to a directory.
     */
    private void validateEntityForBuckets(final HttpEntity entity) {
        final String contentType;

        if (entity.getContentType() != null) {
            contentType = entity.getContentType().getValue();
        } else {
            contentType = null;
        }

        if (DIRECTORY_RESPONSE_CONTENT_TYPE.equals(contentType)) {
            String msg = "A file/directory has been supplied in the bucket listing path. "
                    + "Only the contents of buckets can be listed.";
            MantaUnexpectedObjectTypeException e = new MantaUnexpectedObjectTypeException(msg,
                    ObjectType.BUCKET, ObjectType.DIRECTORY);
            e.setContextValue("path", path);

            try {
                MantaHttpHeaders headers = new MantaHttpHeaders(currentResponse.getAllHeaders());
                e.setResponseHeaders(headers);
            } catch (RuntimeException re) {
                LOG.warn("Unable to convert response headers to MantaHttpHeaders", e);
            }

            throw e;
        }
    }

    /**
     * Generates query after verifying which query parameters like marker, prefix,
     * delimiter are non-null and consequently generating the appropriate executable
     * query for the Manta server.
     *
     * @return desired executable query for Manta Server
     */
    private synchronized String queryGenerator() {
        StringBuilder finalQuery = new StringBuilder(String.format("&limit=%d", limit));

        if (marker != null) {
            finalQuery.append(String.format("&marker=%s", UTF8_URL_ENCODER.encodeQueryElement(marker)));
        }

        prependQueryWithParams(finalQuery);
        finalQuery.deleteCharAt(0).insert(0, "?");
        Validate.isTrue(startsWith(finalQuery, "?"),
                "Executable query incorrectly generated");

        return finalQuery.toString();
    }

    /**
     * Helper method for {@link MantaBucketListingIterator#queryGenerator()}.
     *
     * @param query baseQuery that is to be configured
     */
    private void prependQueryWithParams(final StringBuilder query) {
        Validate.notNull(query, "Query parsed must never be null");

        if (prefix != null) {
            query.insert(0, String.format("&prefix=%s", prefix));
        }
        if (delimiter != null) {
            query.insert(0, String.format("?delimiter=%c", delimiter));
        }
    }

    /**
     * @return total lines processed
     */
    public long getLines() {
        return lines.get();
    }
}
