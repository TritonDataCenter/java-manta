package com.joyent.manta.http;

import com.joyent.manta.util.FailingInputStream;
import com.joyent.manta.util.MantaUtils;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.protocol.HttpContext;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Math.toIntExact;
import static org.apache.commons.lang3.Validate.notNull;
import static org.apache.http.HttpHeaders.CONTENT_LENGTH;
import static org.apache.http.HttpHeaders.CONTENT_RANGE;
import static org.apache.http.HttpHeaders.ETAG;
import static org.apache.http.HttpHeaders.RANGE;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.http.HttpStatus.SC_PARTIAL_CONTENT;
import static org.apache.http.HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE;

class FixedCountEntityFailingByteArrayObjectHttpHandler implements EntityPopulatingHttpRequestHandler {

    protected final byte[] objectContent;

    private final AtomicInteger requestsToFail;

    private final boolean postReadFailure;

    private final ConcurrentHashMap<HttpResponse, HttpEntity> responseEntities;

    private Object lock;

    FixedCountEntityFailingByteArrayObjectHttpHandler(final byte[] objectContent,
                                                      final int requestsToFail,
                                                      final boolean postReadFailure) {
        this.objectContent = objectContent;
        this.requestsToFail = new AtomicInteger(requestsToFail);
        this.postReadFailure = postReadFailure;
        this.responseEntities = new ConcurrentHashMap<>();
        this.lock = new Object();
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
        response.setHeader(ETAG, this.generateETag(this.objectContent));

        InputStream responseBody;
        final long responseLength;
        final Header rangeHeader = request.getFirstHeader(RANGE);
        if (rangeHeader != null) {

            final Long[] reqRange;
            try {
                reqRange = MantaUtils.parseSingleRange(rangeHeader.getValue());
            } catch (final IllegalArgumentException e) {
                response.setStatusCode(SC_REQUESTED_RANGE_NOT_SATISFIABLE);
                return;
            }

            if (reqRange[0] < 0 || objectContent.length <= reqRange[1] || reqRange[1] < reqRange[0]) {
                response.setStatusCode(SC_REQUESTED_RANGE_NOT_SATISFIABLE);
                return;
            }

            // we need to add one since HTTP ranges are inclusive but ByteArrayInputStream's length is not
            final int startInclusive = toIntExact(reqRange[0]);
            final int endInclusive = toIntExact(reqRange[1]);
            responseLength = 1 + endInclusive - startInclusive;

            responseBody = new ByteArrayInputStream(
                    this.objectContent,
                    startInclusive,
                    toIntExact(responseLength));

            final String contentRange = String.format(
                    "bytes %d-%d/%d",
                    reqRange[0],
                    reqRange[1],
                    this.objectContent.length);

            response.setHeader(CONTENT_RANGE, contentRange);
            response.setStatusCode(SC_PARTIAL_CONTENT);
        } else {
            responseBody = new ByteArrayInputStream(this.objectContent);
            responseLength = this.objectContent.length;
        }
        response.setHeader(CONTENT_LENGTH, Long.toString(responseLength));


        if (0 < requestsToFail.getAndDecrement()) {
            // this causes a connection reset on the client side when
            // the exception is triggered
            responseBody = new FailingInputStream(
                    responseBody,
                    Math.floorDiv(this.objectContent.length, 2),
                    postReadFailure);
        }

        responseEntities.put(response, new InputStreamEntity(responseBody, responseLength));
    }

    @Override
    public void populateEntity(final HttpResponse response) {
        final HttpEntity entity = notNull(responseEntities.get(response), "Response entity missing");

        response.setEntity(entity);
    }
}


