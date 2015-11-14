package com.joyent.manta.client;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponse;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

/**
 * @author Elijah Zupancic
 */
public class MantaObjectInputStream extends InputStream implements MantaObject {
    private static final long serialVersionUID = 5300041609801576485L;

    private final MantaObjectMetadata metadata;
    private final InputStream backingStream;
    private final HttpResponse httpResponse;

    MantaObjectInputStream(final MantaObjectMetadata metadata,
                           final HttpResponse httpResponse) throws IOException {
        this.metadata = metadata;
        this.httpResponse = httpResponse;
        this.backingStream = httpResponse.getContent();
    }

    @Override
    public String getPath() {
        return metadata.getPath();
    }

    @Override
    public Long getContentLength() {
        return metadata.getContentLength();
    }

    @Override
    public String getContentType() {
        return metadata.getContentType();
    }

    @Override
    public String getEtag() {
        return metadata.getEtag();
    }

    @Override
    public Date getLastModifiedTime() {
        return metadata.getLastModifiedTime();
    }

    @Override
    public String getMtime() {
        return metadata.getMtime();
    }

    @Override
    public String getType() {
        return metadata.getType();
    }

    @Override
    public HttpHeaders getHttpHeaders() {
        return metadata.getHttpHeaders();
    }

    @Override
    public Object getHeader(final String fieldName) {
        return metadata.getHeader(fieldName);
    }

    @Override
    public boolean isDirectory() {
        return metadata.isDirectory();
    }

    @Override
    public String getRequestId() {
        return metadata.getRequestId();
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
        backingStream.close();
        httpResponse.disconnect();
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
