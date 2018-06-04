package com.joyent.manta.http;

import com.joyent.manta.util.FailingInputStream;
import com.joyent.manta.util.MantaUtils;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.http.Header;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Math.toIntExact;
import static org.apache.http.HttpHeaders.CONTENT_RANGE;
import static org.apache.http.HttpHeaders.ETAG;
import static org.apache.http.HttpHeaders.RANGE;
import static org.apache.http.HttpStatus.SC_OK;

class FixedFailureCountByteArrayObjectHttpHandler implements HttpRequestHandler {

    protected final byte[] objectContent;

    private final AtomicInteger requestsToFail;

    FixedFailureCountByteArrayObjectHttpHandler(final byte[] objectContent, final int requestsToFail) {
        this.objectContent = objectContent;
        this.requestsToFail = new AtomicInteger(requestsToFail);
    }

    protected String generateETag(final byte[] objectContent) {
        return DigestUtils.md5Hex(objectContent);
    }

    @Override
    public void handle(final HttpRequest request,
                       final HttpResponse response,
                       final HttpContext context) {
        if (!HttpGet.METHOD_NAME.equalsIgnoreCase(request.getRequestLine().getMethod())) {
            response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
            return;
        }

        response.setStatusCode(SC_OK);
        // TODO: do something different with ETag?
        response.setHeader(ETAG, this.generateETag(this.objectContent));

        InputStream responseBody;
        final Header rangeHeader = request.getFirstHeader(RANGE);
        if (rangeHeader != null) {
            final Long[] reqRange = MantaUtils.parseSingleRange(rangeHeader.getValue());

            responseBody = new ByteArrayInputStream(
                    this.objectContent,
                    toIntExact(reqRange[0]),
                    toIntExact(reqRange[1]));

            final String contentRange = String.format(
                    "bytes %d-%d/%d",
                    reqRange[0],
                    reqRange[1],
                    this.objectContent.length);

            response.setHeader(CONTENT_RANGE, contentRange);
            response.setStatusCode(HttpStatus.SC_PARTIAL_CONTENT);
        } else {
            responseBody = new ByteArrayInputStream(this.objectContent);
        }


        if (0 < requestsToFail.getAndDecrement()) {
            // this causes a connection reset on the client side when
            // the exception is triggered
            responseBody = new FailingInputStream(
                    responseBody,
                    Math.floorDiv(this.objectContent.length, 2),
                    RandomUtils.nextBoolean());
        }

        response.setEntity(new InputStreamEntity(responseBody, this.objectContent.length));
    }
}


