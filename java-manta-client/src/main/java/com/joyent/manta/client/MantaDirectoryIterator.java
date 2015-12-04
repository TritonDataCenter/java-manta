/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponse;
import com.joyent.manta.exception.MantaObjectException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.joyent.manta.client.MantaObjectResponse.DIRECTORY_RESPONSE_CONTENT_TYPE;
import static com.joyent.manta.client.MantaUtils.formatPath;

/**
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaDirectoryIterator implements Iterator<Map<String, Object>>,
        AutoCloseable {
    private final int pagingSize;

    private final String url;
    private final String path;
    private final HttpHelper httpHelper;
    private final AtomicLong currentLine = new AtomicLong(0);
    private final AtomicLong lines = new AtomicLong(0);
    private final AtomicReference<String> nextLine =
            new AtomicReference<>();
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final ObjectMapper mapper = MantaObjectParser.MAPPER;
    private volatile String lastMarker;
    private volatile BufferedReader br;
    private volatile HttpResponse currentResponse;

    public MantaDirectoryIterator(final String url,
                                  final String path,
                                  final HttpHelper httpHelper,
                                  final int pagingSize) throws IOException {
        this.url = url;
        this.path = path;
        this.httpHelper = httpHelper;

        if (pagingSize < 2) {
            throw new IllegalArgumentException("Paging size must be greater than "
                    + "1 and less than or equal to 1024");
        }

        this.pagingSize = pagingSize;
    }


    private synchronized void selectReader() throws IOException {
        if (lastMarker == null) {
            String query = String.format("?limit=%d", pagingSize);
            GenericUrl genericUrl = new GenericUrl(url + formatPath(path)
                    + query);
            currentResponse = httpHelper.httpGet(genericUrl, null);
            HttpHeaders headers = currentResponse.getHeaders();

            if (!headers.getContentType().contentEquals(DIRECTORY_RESPONSE_CONTENT_TYPE)) {
                String msg = String.format("Expected directory path, but was file path: %s",
                        path);
                throw new MantaObjectException(msg);
            }

            Reader streamReader = new InputStreamReader(currentResponse.getContent(),
                    "UTF-8");
            br = new BufferedReader(streamReader);
        } else {
            String query = String.format("?limit=%d&marker=%s",
                    pagingSize, URLEncoder.encode(lastMarker, "UTF-8"));
            GenericUrl genericUrl = new GenericUrl(url + formatPath(path)
                + query);

            try {
                br.close();
                currentResponse.disconnect();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            currentResponse = httpHelper.httpGet(genericUrl, null);
            Reader streamReader = new InputStreamReader(currentResponse.getContent(),
                    "UTF-8");
            br = new BufferedReader(streamReader);

            // Reset count of lines for the current stream
            currentLine.set(0);

            // We read one line to clear it because it is our marker
            br.readLine();
        }

        nextLine.set(br.readLine());
        lines.incrementAndGet();
        currentLine.incrementAndGet();

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
            currentLine.incrementAndGet();

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

            Objects.requireNonNull(name, "Name must be present in JSON input");

            this.lastMarker = name;

            return lookup;

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        try {
            if (br != null) {
                br.close();
            }

            if (currentResponse != null) {
                currentResponse.disconnect();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
