/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.conn.EofSensorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

/**
 * {@link InputStream} implementation that wraps the input stream provided
 * from {@link MantaClient} and implements {@link MantaObject} so that you
 * can obtain metadata information.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaObjectInputStream extends InputStream implements MantaObject,
        AutoCloseable {
    private static final long serialVersionUID = -4692104903008485259L;

    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(MantaObjectInputStream.class);

    /**
     * Response from request to the Manta API.
     */
    private final MantaObjectResponse response;

    /**
     * The backing {@link InputStream} implementation.
     */
    private final EofSensorInputStream backingStream;

    /**
     * The HTTP response sent from the Manta API.
     */
    private final transient CloseableHttpResponse httpResponse;

    /**
     * Create a new instance from the results of a GET HTTP call to the
     * Manta API.
     *
     * @param response Metadata object built from request
     * @param httpResponse Response object created
     * @param backingStream Underlying stream being wrapped
     */
    public MantaObjectInputStream(final MantaObjectResponse response,
                                  final CloseableHttpResponse httpResponse,
                                  final EofSensorInputStream backingStream) {
        this.backingStream = backingStream;
        this.response = response;
        this.httpResponse = httpResponse;
    }

    /**
     * Creates a new instance based on an existing instance.
     * @param copy instance to copy properties from
     */
    @SuppressWarnings("unchecked")
    protected MantaObjectInputStream(final MantaObjectInputStream copy) {
        this.backingStream = (EofSensorInputStream)copy.getBackingStream();
        this.response = copy.response;
        this.httpResponse = copy.httpResponse;
    }

    @Override
    public String getPath() {
        return response.getPath();
    }

    @Override
    public Long getContentLength() {
        return response.getContentLength();
    }

    @Override
    public String getContentType() {
        return response.getContentType();
    }

    @Override
    public String getEtag() {
        return response.getEtag();
    }

    @Override
    public Date getLastModifiedTime() {
        return response.getLastModifiedTime();
    }

    @Override
    public String getMtime() {
        return response.getMtime();
    }

    @Override
    public String getType() {
        return response.getType();
    }

    @Override
    public MantaHttpHeaders getHttpHeaders() {
        return response.getHttpHeaders();
    }

    @Override
    public Object getHeader(final String fieldName) {
        return response.getHeader(fieldName);
    }

    @Override
    public String getHeaderAsString(final String fieldName) {
        return response.getHeaderAsString(fieldName);
    }

    @Override
    public MantaMetadata getMetadata() {
        return response.getMetadata();
    }

    @Override
    public byte[] getMd5Bytes() {
        return response.getMd5Bytes();
    }

    @Override
    public boolean isDirectory() {
        return response.isDirectory();
    }

    @Override
    public String getRequestId() {
        return response.getRequestId();
    }

    protected MantaObjectResponse getResponse() {
        return response;
    }

    /**
     * Returns the HTTP Client response object. This may be useful for debugging.
     *
     * @return the underlying HTTP response object
     */
    public Object getHttpResponse() {
        return httpResponse;
    }

    public InputStream getBackingStream() {
        return this.backingStream;
    }

    @Override
    public int read() throws IOException {
        return backingStream.read();
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        return backingStream.read(b, off, len);
    }

    @Override
    public int read(final byte[] b) throws IOException {
        return backingStream.read(b);
    }

    @Override
    public int available() throws IOException {
        return backingStream.available();
    }

    @Override
    public void close() throws IOException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Closing backingStream {} and response {}",
                    this.backingStream, httpResponse);
        }

        IOUtils.closeQuietly(backingStream);
        IOUtils.closeQuietly(httpResponse);
    }

    @Override
    public void mark(final int readlimit) {
        backingStream.mark(readlimit);
    }

    @Override
    public void reset() throws IOException {
        backingStream.reset();
    }

    @Override
    public boolean markSupported() {
        return backingStream.markSupported();
    }

    /**
     * Aborts this stream.
     * This is a special version of {@link #close close()} which prevents
     * re-use of the underlying connection, if any. Calling this method
     * indicates that there should be no attempt to read until the end of
     * the stream.
     * @throws IOException thrown when unable to abort connection
     */
    public void abortConnection() throws IOException {
        backingStream.abortConnection();
    }
}
