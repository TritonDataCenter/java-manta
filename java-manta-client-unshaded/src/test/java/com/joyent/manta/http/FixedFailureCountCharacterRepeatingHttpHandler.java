package com.joyent.manta.http;

import com.joyent.manta.util.FailingInputStream;
import com.joyent.manta.util.MantaUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.apache.http.HttpHeaders.CONTENT_RANGE;
import static org.apache.http.HttpHeaders.ETAG;
import static org.apache.http.HttpHeaders.RANGE;
import static org.apache.http.HttpStatus.SC_OK;

class FixedFailureCountCharacterRepeatingHttpHandler implements HttpRequestHandler {


    static final String HEADER_INPUT_CHARACTER = "x-input-character";

    static final String HEADER_REPEAT_COUNT = "x-repeat-count";

    private final AtomicInteger requestsToFail;

    FixedFailureCountCharacterRepeatingHttpHandler(final int requestsToFail) {
        this.requestsToFail = new AtomicInteger(requestsToFail);
    }

    @Override
    public void handle(final HttpRequest request,
                       final HttpResponse response,
                       final HttpContext context) {
        if (!request.containsHeader(HEADER_INPUT_CHARACTER)
                && !request.containsHeader(HEADER_REPEAT_COUNT)) {
            response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
            return;
        }

        response.setStatusCode(SC_OK);
        final String inputChar = request.getFirstHeader(HEADER_INPUT_CHARACTER).getValue();
        final Integer repeatCount = Integer.parseInt(request.getFirstHeader(HEADER_REPEAT_COUNT).getValue());
        final String rawResponse = StringUtils.repeat(inputChar, repeatCount);
        final byte[] responseBytes = rawResponse.getBytes(US_ASCII);

        // TODO: do something different with ETag?
        response.setHeader(ETAG, rawResponse);

        InputStream responseBody;
        final Header rangeHeader = request.getFirstHeader(RANGE);
        if (rangeHeader != null) {
            final Long[] reqRange = MantaUtils.parseSingleRange(rangeHeader.getValue());

            responseBody = new ByteArrayInputStream(
                    responseBytes,
                    toIntExact(reqRange[0]),
                    toIntExact(reqRange[1]));

            final String contentRange = String.format("bytes %d-%d/%d", reqRange[0], reqRange[1], responseBytes.length);
            response.setHeader(CONTENT_RANGE, contentRange);
            response.setStatusCode(HttpStatus.SC_PARTIAL_CONTENT);
        } else {
            responseBody = new ByteArrayInputStream(responseBytes);
        }


        if (0 < requestsToFail.getAndDecrement()) {
            // this causes a connection reset on the client side when
            // the exception is triggered
            responseBody = new FailingInputStream(
                    responseBody,
                    Math.floorDiv(responseBytes.length, 2),
                    RandomUtils.nextBoolean());
        }


        response.setEntity(new InputStreamEntity(responseBody, responseBytes.length));
    }
}


