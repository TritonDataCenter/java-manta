/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.joyent.manta.exception.MantaObjectException;
import com.joyent.manta.http.HttpHelper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.Validate;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;

import java.io.BufferedReader;
import java.io.IOException;
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

import static com.joyent.manta.client.MantaObjectResponse.DIRECTORY_RESPONSE_CONTENT_TYPE;
import static com.joyent.manta.util.MantaUtils.formatPath;

/**
 * <p>Class that wraps the paging of directory listing in Manta to a single
 * continuous iterator.</p>
 *
 * <p><a href="https://apidocs.joyent.com/manta/api.html#ListDirectory">
 * Listing Manta directories</a> is done by getting pages of data with N number
 * of records per page. Each page of data is requested using a limit (number
 * of records) and a marker (the last seen item in the list). This class
 * automates that process and abstracts out the details of the paging process.</p>
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaDirectoryListingIterator implements Iterator<Map<String, Object>>,
        AutoCloseable {
    /**
     * Size of result set requested against the Manta API (2-1024).
     */
    private final int pagingSize;

    /**
     * Base Manta URL that all paths are appended to.
     */
    private final String url;

    /**
     * Path to directory in which we will iterate through its contents.
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
     * Create a new instance of a directory list iterator.
     *
     * @param url base Manta URL that all paths are appended to
     * @param path path to directory in which we will iterate through its contents
     * @param httpHelper HTTP request helper class
     * @param pagingSize size of result set requested against the Manta API (2-1024).
     */
    public MantaDirectoryListingIterator(final String url,
                                         final String path,
                                         final HttpHelper httpHelper,
                                         final int pagingSize) {
        Validate.notNull(url, "URL must not be null");
        Validate.notNull(path, "Path must not be null");
        Validate.notNull(httpHelper, "HTTP help must not be null");

        this.url = url;
        this.path = path;
        this.httpHelper = httpHelper;

        if (pagingSize < 2) {
            throw new IllegalArgumentException("Paging size must be greater than "
                    + "1 and less than or equal to 1024");
        }

        this.pagingSize = pagingSize;
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
            String query = String.format("?limit=%d", pagingSize);
            String uri = url + formatPath(path) + query;
            HttpGet get = new HttpGet(uri);

            IOUtils.closeQuietly(currentResponse);
            currentResponse = httpHelper.executeRequest(get, null);
            HttpEntity entity = currentResponse.getEntity();
            String contentType = entity.getContentType().getValue();

            if (!contentType.equals(DIRECTORY_RESPONSE_CONTENT_TYPE)) {
                String msg = String.format("Expected directory path, but was file path: %s",
                        path);
                throw new MantaObjectException(msg);
            }

            Reader streamReader = new InputStreamReader(entity.getContent(),
                    StandardCharsets.UTF_8.name());
            br = new BufferedReader(streamReader);
        } else {
            String query = String.format("?limit=%d&marker=%s",
                    pagingSize, URLEncoder.encode(lastMarker, "UTF-8"));
            String uri = url + formatPath(path) + query;
            HttpGet get = new HttpGet(uri);

            IOUtils.closeQuietly(br);
            IOUtils.closeQuietly(currentResponse);

            currentResponse = httpHelper.executeRequest(get, null);
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
        if (finished.get()) {
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

            this.lastMarker = name;

            return lookup;

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(br);
        IOUtils.closeQuietly(currentResponse);
    }

    /**
     * @return total lines processed
     */
    public long getLines() {
        return lines.get();
    }
}
