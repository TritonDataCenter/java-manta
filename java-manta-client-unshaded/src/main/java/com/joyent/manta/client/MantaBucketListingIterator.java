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
import com.joyent.manta.exception.MantaIOException;
import com.joyent.manta.exception.MantaResourceCloseException;
import com.joyent.manta.http.HttpHelper;
import com.joyent.manta.util.MantaUtils;
import org.apache.commons.lang3.Validate;
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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.joyent.manta.util.MantaUtils.formatPath;

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
@SuppressWarnings("Duplicates")
public class MantaBucketListingIterator implements Iterator<Map<String, Object>>,
        AutoCloseable  {
    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(MantaBucketListingIterator.class);

    /**
     * Maximum number of results to return for a bucket listing.
     */
    static final int MAX_RESULTS = 1024;

    /**
     * Maximum number of results to return for a bucket listing.
     */
    static final String DEFAULT_PREFIX = "";

    /**
     * Boolean value indicating whether we have an ordered bucket listing.
     */
    private final boolean isSorted;

    /**
     * Size of result set requested against the Manta API (2-1024).
     */
    private final String prefix;

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
     * The last marker we used to request against the Manta API.
     */
    private volatile String lastMarker;

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
     * @param isSorted true for lexicographical ordering of buckets
     * @param prefix filter for listing buckets
     */
    public MantaBucketListingIterator(final String path,
                                      final HttpHelper httpHelper,
                                      final boolean isSorted,
                                      final String prefix) {
        Validate.notBlank(path, "Path must not be blank");
        Validate.notNull(httpHelper, "HTTP help must not be null");

        this.path = path;
        this.httpHelper = httpHelper;

        if (MantaUtils.parseBooleanOrNull(isSorted)) {
            throw new IllegalArgumentException("Invalid query parameter supplied"
                    + "for sorted buckets listing");
        }

        // Conditional check for prefix if needed

        this.isSorted = isSorted;
        this.prefix = prefix;
    }

    /**
     * Create a new instance of a bucket list iterator.
     *
     * @param url ignored
     * @param path path to bucket in which we will iterate through its contents
     * @param httpHelper HTTP request helper class
     */
    public MantaBucketListingIterator(final String url,
                                         final String path,
                                         final HttpHelper httpHelper) {
        this(path, httpHelper, false, DEFAULT_PREFIX);
    }

    /**
     * Create a new instance of a bucket list iterator.
     *
     * @param path path to bucket in which we will iterate through its contents
     * @param httpHelper HTTP request helper class
     */
    public MantaBucketListingIterator(final String path,
                                         final HttpHelper httpHelper) {
        this(path, httpHelper, false, DEFAULT_PREFIX);
    }


    /**
     * Chooses the next reader by opening a HTTP connection to get the next
     * page of input from the Manta API. If there isn't another page of data
     * available, we mark ourselves as finished.
     *
     * @throws IOException thrown when we can't successfully open an HTTP connection
     */
    private synchronized void selectReader() throws IOException {
        if (lastMarker == null) {
            String query = String.format("?sorted=%b&prefix=%s&limit=%d",
                    isSorted, prefix, MAX_RESULTS);
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

            InputStream contentStream = entity.getContent();
            Objects.requireNonNull(contentStream, "A bucket listing without "
                    + "content is not valid. Content is null.");

            Reader streamReader = new InputStreamReader(entity.getContent(),
                    StandardCharsets.UTF_8.name());
            br = new BufferedReader(streamReader);
        } else {
            String query = String.format("?limit=%d&marker=%s",
                    MAX_RESULTS, URLEncoder.encode(lastMarker, "UTF-8"));
            final HttpGet request = httpHelper.getRequestFactory().get(formatPath(path) + query);

            closeResources();

            currentResponse = httpHelper.executeRequest(request, null);
            HttpEntity entity = currentResponse.getEntity();
            Reader streamReader = new InputStreamReader(entity.getContent(),
                    StandardCharsets.UTF_8.name());
            br = new BufferedReader(streamReader);

            // We read one line to clear it because it is our marker
            br.readLine();
        }

        nextLine.set(br.readLine());
        lines.incrementAndGet();

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

            this.lastMarker = name;

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
     * @return total lines processed
     */
    public long getLines() {
        return lines.get();
    }
}
