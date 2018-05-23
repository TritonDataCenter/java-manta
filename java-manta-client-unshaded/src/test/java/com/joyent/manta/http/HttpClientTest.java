package com.joyent.manta.http;

import com.joyent.manta.exception.RecoverableDownloadMantaIOException;
import com.joyent.manta.util.MultipartInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.StandardHttpRequestRetryHandler;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static com.joyent.manta.http.FixedFailureCountCharacterRepeatingHttpHandler.HEADER_INPUT_CHARACTER;
import static com.joyent.manta.http.FixedFailureCountCharacterRepeatingHttpHandler.HEADER_REPEAT_COUNT;
import static java.lang.System.out;
import static org.apache.commons.lang3.Validate.notNull;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.http.HttpStatus.SC_PARTIAL_CONTENT;

@Test
public class HttpClientTest {

    private static final Logger LOG = LoggerFactory.getLogger(HttpClientTest.class);

    private static final String CONTEXT_RESUMABLE_DOWNLOAD_ENABLED = "manta.resumable_download_enabled";
    private static final String CONTEXT_RESUMABLE_DOWNLOAD = "manta.resumable_download";

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
        ctx.setAttribute(CONTEXT_RESUMABLE_DOWNLOAD_ENABLED, true);

        int loops = 0;
        boolean finished = false;
        do {
            loops++;
            final CloseableHttpResponse res = client.execute(req, ctx);
            final int statusCode = res.getStatusLine().getStatusCode();
            if (statusCode != SC_OK
                    && statusCode != SC_PARTIAL_CONTENT) {
                throw new AssertionError("invalid response code: " + statusCode);
            }

            out.println("response: " + res.getStatusLine());
            final HttpEntity ent = res.getEntity();
            if (ent == null) {
                throw new AssertionError("response must have entity");
            }

            final MultipartInputStream multiInput = resumed.getMultipartInputStream();
            multiInput.setSource(ent.getContent());

            try {
                IOUtils.copy(multiInput, savedEntity, 4);
                finished = true;
            } catch (final IOException e) {
                if (RecoverableDownloadMantaIOException.isResumableError(e)) {
                    continue;
                }

                throw e;
            } finally {
                res.close();
            }
        } while (!finished && loops < 5);

        if (!finished) {
            throw new AssertionError("Failed to download object content, attempts: " + loops);
        }
    }

    private CloseableHttpClient prepareClient(final HttpClientConnection conn) {
        return HttpClientBuilder.create()
                .setConnectionManager(new FakeHttpClientConnectionManager(conn))
                .setRetryHandler(new StandardHttpRequestRetryHandler())
                .addInterceptorFirst((HttpRequestInterceptor) (request, context) -> {
                    if (request.getRequestLine() == null) {
                        // nonsensical but might as well
                        return;
                    }

                    if (!HttpGet.METHOD_NAME.equals(request.getRequestLine().getMethod())) {
                        // not a GET request
                        return;
                    }

                    final Object resumableDownload = context.getAttribute(CONTEXT_RESUMABLE_DOWNLOAD);

                    if (null != request.getFirstHeader(HttpHeaders.IF_MATCH)) {
                        // request already uses if-match, don't clobber it
                        return;
                    }

                    if (null == resumableDownload) {
                        // prepare the context to receive a marker which will be set once the response arrives
                        context.setAttribute(CONTEXT_RESUMABLE_DOWNLOAD, true);
                        return;
                    }

                    if (resumableDownload instanceof Boolean) {
                        // this interceptor has somehow run twice for the same initial download
                        LOG.debug("ResumableDownloadPreparationInterceptor has been invoked unexpectedly, skipping!");
                        return;
                    }

                    if (!(resumableDownload instanceof ResumableDownloadMarker)) {
                        LOG.error("Unexpected type found while preparing resumable download, got: " + resumableDownload.getClass());

                        return;
                    }

                    final ResumableDownloadMarker resuming = (ResumableDownloadMarker) resumableDownload;
                })
                .addInterceptorFirst((HttpResponseInterceptor) (response, context) -> {
                    final Object resumableDownload = context.getAttribute(CONTEXT_RESUMABLE_DOWNLOAD);
                    if (resumableDownload == null) {
                        // the request is not eligible for resuming
                        return;
                    }

                    final ImmutablePair<String, ContentRange> fingerprint = ResumableDownloadMarker.extractFingerprint(response);

                    if (fingerprint == null) {
                        // there is not enough information in the response to create or update a marker
                        // so clear one that may already be present and exit
                        context.removeAttribute(CONTEXT_RESUMABLE_DOWNLOAD);
                        return;
                    }

                    if (!(resumableDownload instanceof ResumableDownloadMarker)) {
                        // we are receiving the first response, attach a new marker to the context and exit
                        context.setAttribute(
                                CONTEXT_RESUMABLE_DOWNLOAD,
                                new ResumableDownloadMarker(fingerprint.left, fingerprint.right));
                        return;
                    }

                    // check that the marker is still valid
                    final ResumableDownloadMarker resumed = (ResumableDownloadMarker) resumableDownload;

                    if (!fingerprint.left.equals(resumed.getEtag())) {
                        // the etag has changed, abort!
                        context.removeAttribute(CONTEXT_RESUMABLE_DOWNLOAD);
                    }

                })
                .build();
    }



}
