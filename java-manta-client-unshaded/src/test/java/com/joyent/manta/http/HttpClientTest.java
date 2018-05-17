package com.joyent.manta.http;

import com.joyent.manta.exception.RecoverableDownloadMantaIOException;
import com.joyent.manta.util.FailingInputStream;
import com.joyent.manta.util.MantaUtils;
import com.joyent.manta.util.MultipartInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.StandardHttpRequestRetryHandler;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Math.toIntExact;
import static java.lang.System.out;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.apache.commons.lang3.Validate.notNull;
import static org.apache.http.HttpHeaders.RANGE;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.http.HttpStatus.SC_PARTIAL_CONTENT;

@Test
public class HttpClientTest {

    private static final Logger LOG = LoggerFactory.getLogger(HttpClientTest.class);

    private static final String CONTEXT_RESUMABLE_DOWNLOAD_MEMENTO = "manta.resumable_download_memento";

    private static final String HEADER_INPUT_CHARACTER = "x-input-character";

    private static final String HEADER_REPEAT_COUNT = "x-repeat-count";

    private static class ResumableDownloadMemento {
        private final String etag;
        private final Long contentLength;
        private final MultipartInputStream multipartInputStream;

        ResumableDownloadMemento(final String etag,
                                 final Long contentLength) {
            this.etag = etag;
            this.contentLength = contentLength;
            this.multipartInputStream = new MultipartInputStream();
        }

        public String getEtag() {
            return this.etag;
        }

        public Long getContentLength() {
            return this.contentLength;
        }

        public MultipartInputStream getMultipartInputStream() {
            return this.multipartInputStream;
        }
    }

    public void testServer() throws Exception {

        final CloseableHttpClient client = prepareClient(
                new FakeHttpClientConnection(
                        new FixedFailureCountCharacterRepeatingHttpHandler(1)));

        final HttpUriRequest req = new HttpGet("http://localhost");

        final int expectedEntityLength = 16;
        req.setHeader(HEADER_REPEAT_COUNT, Integer.toString(expectedEntityLength));
        req.setHeader(HEADER_INPUT_CHARACTER, "a");

        final OutputStream savedEntity = new ByteArrayOutputStream();

        final HttpContext ctx = new HttpClientContext();

        try (final CloseableHttpResponse res = client.execute(req, ctx)) {
            final int statusCode = res.getStatusLine().getStatusCode();
            if (statusCode != SC_OK
                    && statusCode != SC_PARTIAL_CONTENT) {
                throw new AssertionError("invalid response code: " + res.getStatusLine().getStatusCode());
            }

            out.println("response: " + res.getStatusLine());
            final HttpEntity ent = res.getEntity();
            if (ent == null) {
                throw new AssertionError("response must have entity");
            }

            attemptToReadEntity(ctx, ent.getContent(), savedEntity);

        } catch (final RecoverableDownloadMantaIOException recoverable) {
            // TODO: do something here
            LOG.info("success!");
        } catch (final IOException e) {
            LOG.info("failure!");
            throw e;
        }
    }

    private void attemptToReadEntity(final HttpContext context,
                                     final InputStream content,
                                     final OutputStream copy) throws IOException {
        try {
            IOUtils.copy(content, copy, 4);
        } catch (final IOException e) {
            if (RecoverableDownloadMantaIOException.isResumableError(e)) {
                throw new RecoverableDownloadMantaIOException(e);
            }

            throw e;
        }
    }


    private CloseableHttpClient prepareClient(final HttpClientConnection conn) {
        return HttpClientBuilder.create()
                .setConnectionManager(new FakeHttpClientConnectionManager(conn))
                .setRetryHandler(new StandardHttpRequestRetryHandler())
                .addInterceptorFirst((HttpResponseInterceptor) (response, context) -> {
                    final HttpEntity responseEntity = response.getEntity();

                    if (responseEntity == null) {
                        return;
                    }

                    final Object existingMemento = context.getAttribute(CONTEXT_RESUMABLE_DOWNLOAD_MEMENTO);

                    if (existingMemento == null) {
                        final String etag = notNull(response.getFirstHeader(HttpHeaders.ETAG)).getValue();
                        final Long contentLength = responseEntity.getContentLength();

                        context.setAttribute(
                                CONTEXT_RESUMABLE_DOWNLOAD_MEMENTO,
                                new ResumableDownloadMemento(etag, contentLength));

                    } else if (existingMemento instanceof ResumableDownloadMemento) {
                        throw new NotImplementedException("update? ¯\\_(ツ)_/¯");
                    } else {
                        throw new AssertionError("wtf? ¯\\_(ツ)_/¯");
                    }
                })
                .build();
    }

    private class FixedFailureCountCharacterRepeatingHttpHandler implements HttpRequestHandler {

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
            response.setHeader(HttpHeaders.ETAG, rawResponse);

            InputStream responseBody;
            final Header rangeHeader = request.getFirstHeader(RANGE);
            if (rangeHeader != null) {
                final Long[] reqRange = MantaUtils.parseSingleRange(rangeHeader.getValue());

                responseBody = new ByteArrayInputStream(
                        responseBytes,
                        toIntExact(reqRange[0]),
                        toIntExact(reqRange[1]));
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


}
