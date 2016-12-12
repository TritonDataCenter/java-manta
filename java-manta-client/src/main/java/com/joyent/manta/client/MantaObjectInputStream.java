/*
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
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
public class MantaObjectInputStream extends InputStream implements MantaObject {
    private static final long serialVersionUID = 4129729453592380566L;

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
    private final InputStream backingStream;

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
    MantaObjectInputStream(final MantaObjectResponse response,
                           final CloseableHttpResponse httpResponse,
                           final InputStream backingStream) {
        this.response = response;
        this.httpResponse = httpResponse;
        this.backingStream = backingStream;
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

    @Override
    public int read() throws IOException {
        return backingStream.read();
    }

    @Override
    public int read(final byte[] b) throws IOException {
        return backingStream.read(b);
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        return backingStream.read(b, off, len);
    }

    @Override
    public long skip(final long n) throws IOException {
        return backingStream.skip(n);
    }

    @Override
    public int available() throws IOException {
        return backingStream.available();
    }

    @Override
    public void close() throws IOException {
        LOGGER.debug("Closing backingStream {} and response {}",
                backingStream, httpResponse);

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
}
