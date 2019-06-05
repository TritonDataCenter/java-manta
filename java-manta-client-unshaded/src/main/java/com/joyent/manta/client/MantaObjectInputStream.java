/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.exception.MantaIOException;
import com.joyent.manta.exception.MantaResourceCloseException;
import com.joyent.manta.http.HttpHelper;
import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.conn.EofSensorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

/**
 * {@link InputStream} implementation that wraps the input stream provided from {@link MantaClient} and implements
 * {@link MantaObject} so that you can obtain metadata information.
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
    private final InputStream backingStream;

    /**
     * The HTTP response sent from the Manta API.
     */
    private final transient CloseableHttpResponse httpResponse;

    /**
     * Create a new instance from the results of a GET HTTP call to the Manta API.
     *
     * @param response Metadata object built from request
     * @param httpResponse Response object created
     * @param backingStream Underlying stream being wrapped
     */
    public MantaObjectInputStream(final MantaObjectResponse response,
                                  final CloseableHttpResponse httpResponse,
                                  final InputStream backingStream) {
        this.backingStream = backingStream;
        this.response = response;
        this.httpResponse = httpResponse;
    }

    /**
     * Creates a new instance based on an existing instance.
     *
     * @param copy instance to copy properties from
     */
    protected MantaObjectInputStream(final MantaObjectInputStream copy) {
        this(copy.response, copy.httpResponse, copy.backingStream);
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
    public boolean isBucket() {
        return response.isBucket();
    }

    @Override
    public boolean isBucketObject() {
        return response.isBucketObject();
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

        try {
            if (backingStream != null) {
                backingStream.close();
            }
        } catch (IOException e) {
            /* Since we will be throwing an exception when closing the backing
             * stream, we will only log the error thrown when closing the
             * response because we can only throw up a single exception. */
            try {
                httpResponse.close();
            } catch (IOException ioe) {
                MantaIOException responseMio = new MantaIOException(ioe);
                HttpHelper.annotateContextedException(responseMio, null, httpResponse);
                LOGGER.error("Unable to close HTTP response resource", responseMio);
            }

            MantaIOException mio = new MantaIOException(e);
            HttpHelper.annotateContextedException(mio, null, httpResponse);
            throw mio;
        }

        try {
            httpResponse.close();
        } catch (IOException e) {
            MantaIOException mio = new MantaIOException(e);
            HttpHelper.annotateContextedException(mio, null, httpResponse);
            throw mio;
        }
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
     * <p>Aborts this stream.</p>
     *
     * <p>This is a special version of {@link #close close()} which prevents
     * re-use of the underlying connection, if any. Calling this method
     * indicates that there should be no attempt to read until the end of
     * the stream.</p>
     *
     * <p>If the backing stream of the connection is not a
     * {@link EofSensorInputStream}, this method with call {@link #close()}.</p>
     *
     * <p>This method is deprecated because we can't relay on the underlying
     * backing stream to always be a {@link EofSensorInputStream}.</p>
     *
     * @throws IOException thrown when unable to abort connection
     */
    @Deprecated
    public void abortConnection() throws IOException {
        if (backingStream instanceof EofSensorInputStream) {
            ((EofSensorInputStream) backingStream).abortConnection();
            try {
                if (httpResponse != null) {
                    httpResponse.close();
                }
            } catch (IOException e) {
                MantaIOException mio = new MantaResourceCloseException(e);
                HttpHelper.annotateContextedException(mio, null, httpResponse);
                LOGGER.error("Unable to close HTTP response object", mio);
            }
        } else {
            close();
        }
    }
}
