package com.joyent.manta.http;

import com.joyent.manta.exception.ResumableDownloadIncompatibleRequestException;
import com.joyent.manta.exception.ResumableDownloadUnexpectedResponseException;
import com.joyent.manta.util.ResumableInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.http.Header;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpMessage;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.RequestLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.StandardHttpRequestRetryHandler;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

import static com.joyent.manta.util.MantaUtils.unmodifiableMap;
import static java.lang.System.out;
import static java.util.Collections.emptyMap;
import static org.apache.http.HttpHeaders.CONTENT_LENGTH;
import static org.apache.http.HttpHeaders.CONTENT_RANGE;
import static org.apache.http.HttpHeaders.ETAG;
import static org.apache.http.HttpHeaders.IF_MATCH;
import static org.apache.http.HttpHeaders.RANGE;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.http.HttpStatus.SC_PARTIAL_CONTENT;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

@Test
public class ResumableDownloadCoordinatorTest {

    private static final int FIVE_MB = 5242880;

    private static final Logger LOG = LoggerFactory.getLogger(ResumableDownloadCoordinatorTest.class);

    public void coordinatorsRefuseToShareContext() {
        // mocking the context doesn't make sense since it wouldn't receive updates
        final HttpContext ctx = new BasicHttpContext();

        new ResumableDownloadCoordinator(ctx);

        assertThrows(IllegalArgumentException.class, () ->
                new ResumableDownloadCoordinator(ctx));
    }

    public void ctorRejectsNullInput() {
        assertThrows(NullPointerException.class, () -> new ResumableDownloadCoordinator(null));
    }

    public void enhanceRejectsNullInput() {
        final ResumableDownloadCoordinator coordinator = new ResumableDownloadCoordinator(mock(HttpContext.class));

        assertThrows(NullPointerException.class, () -> coordinator.enhance(null));
    }

    public void coordinatorWith200ResponseMarkerIsInProgress() throws Exception {
        final ResumableDownloadCoordinator coordinator = new ResumableDownloadCoordinator(mock(HttpContext.class));

        assertFalse(coordinator.inProgress());

        coordinator.createMarker(
                prepareResponseWithHeaders(
                        unmodifiableMap(
                                ETAG, singleValueHeader(ETAG, "abc"),
                                CONTENT_LENGTH, singleValueHeader(CONTENT_LENGTH, "2"))));

        assertTrue(coordinator.inProgress());
    }

    public void coordinatorWith206ResponseMarkerIsInProgress() throws Exception {
        final ResumableDownloadCoordinator coordinator = new ResumableDownloadCoordinator(mock(HttpContext.class));

        assertFalse(coordinator.inProgress());

        coordinator.createMarker(
                prepareResponseWithHeaders(
                        unmodifiableMap(
                                ETAG, singleValueHeader(ETAG, "abc"),
                                CONTENT_LENGTH, singleValueHeader(CONTENT_LENGTH, "2"),
                                CONTENT_RANGE, singleValueHeader(CONTENT_RANGE, new HttpRange.Response(0, 1, 2).render()))));

        assertTrue(coordinator.inProgress());
    }

    public void validatesNoCollidingHeadersForInitialRequest() throws Exception {
        validatesCompatibleHeadersForInitialRequest(emptyMap());
    }

    public void validatesCompatibleIfMatchHeaderForInitialRequest() throws Exception {
        final String ifMatchHeaderValue = new HttpRange.Request(0, 10).render();

        // users are allowed to omit or set their own if-match header
        validatesCompatibleHeadersForInitialRequest(
                unmodifiableMap(IF_MATCH, new Header[]{}));
        validatesCompatibleHeadersForInitialRequest(
                unmodifiableMap(IF_MATCH, singleValueHeader(IF_MATCH, ifMatchHeaderValue)));

        // this should never occur so we consider it programmer error, something has been subclassed incorrectly
        assertThrows(ResumableDownloadIncompatibleRequestException.class, () -> {
            validatesCompatibleHeadersForInitialRequest(
                    unmodifiableMap(IF_MATCH, new Header[]{null}));
        });

        // the following is most likely user error, though we should ideally disallow it
        // (if-match: "" doesn't make sense for GET, only for PUT, and this is all about resumable *downloads*)
        assertThrows(ResumableDownloadIncompatibleRequestException.class, () -> {
            validatesCompatibleHeadersForInitialRequest(
                    unmodifiableMap(RANGE, singleValueHeader(RANGE, "")));
        });

        // we make no effort to merge headers
        assertThrows(ResumableDownloadIncompatibleRequestException.class, () -> {
            validatesCompatibleHeadersForInitialRequest(
                    unmodifiableMap(IF_MATCH, new Header[]{new BasicHeader(IF_MATCH, ifMatchHeaderValue), new BasicHeader(IF_MATCH, ifMatchHeaderValue)}));
        });
    }

    public void validatesCompatibleRangeHeaderForInitialRequest() throws Exception {
        final String rangeHeaderValue = new HttpRange.Request(0, 10).render();

        // users are allowed to omit or set their own initial range header
        validatesCompatibleHeadersForInitialRequest(
                unmodifiableMap(RANGE, new Header[]{}));
        validatesCompatibleHeadersForInitialRequest(
                unmodifiableMap(RANGE, singleValueHeader(RANGE, rangeHeaderValue)));

        // this should never occur so we consider it programmer error, something has been subclassed incorrectly
        assertThrows(ResumableDownloadIncompatibleRequestException.class, () -> {
            validatesCompatibleHeadersForInitialRequest(
                    unmodifiableMap(RANGE, new Header[]{null}));
        });

        // the following are most likely user error
        assertThrows(ResumableDownloadIncompatibleRequestException.class, () -> {
            validatesCompatibleHeadersForInitialRequest(
                    unmodifiableMap(RANGE, singleValueHeader(RANGE, "")));
        });

        final ResumableDownloadIncompatibleRequestException malformed = expectThrows(ResumableDownloadIncompatibleRequestException.class, () -> {
            validatesCompatibleHeadersForInitialRequest(
                    unmodifiableMap(RANGE, singleValueHeader(RANGE, "duck")));
        });

        assertTrue(malformed.getCause() != null && malformed.getCause() instanceof HttpException);
        assertTrue(malformed.getMessage().contains("duck"));

        // we explicitly do not handle resuming multiple ranges (though we could in the future)
        // (supplying the same range header twice is also an error, but we're not concerned with that)
        assertThrows(ResumableDownloadIncompatibleRequestException.class, () -> {
            validatesCompatibleHeadersForInitialRequest(
                    unmodifiableMap(RANGE, new Header[]{new BasicHeader(RANGE, rangeHeaderValue), new BasicHeader(RANGE, rangeHeaderValue)}));
        });
    }

    public void complainsAboutBothHeadersWhenRangeAndIfMatchAreMalformed() throws Exception {
        final ResumableDownloadIncompatibleRequestException e = expectThrows(ResumableDownloadIncompatibleRequestException.class, () -> {
            validatesCompatibleHeadersForInitialRequest(
                    unmodifiableMap(
                            RANGE, singleValueHeader(RANGE, ""),
                            IF_MATCH, singleValueHeader(IF_MATCH, "")));
        });

        assertTrue(e.getMessage().contains(RANGE));
        assertTrue(e.getMessage().contains(IF_MATCH));
    }

    private void validatesCompatibleHeadersForInitialRequest(final Map<String, Header[]> headers) throws Exception {
        final HttpGet req = prepareRequestWithHeaders(headers);
        verifyNoMoreInteractions(req);

        final HttpContext ctx = new BasicHttpContext();
        new ResumableDownloadCoordinator(ctx).enhance(req);
    }

    public void refusesToEnhanceNonGetRequests() throws Exception {
        final Class<? extends HttpRequest>[] requests = new Class[]{
                HttpPut.class,
                HttpPost.class,
                HttpDelete.class,
                HttpHead.class,
                HttpOptions.class,
        };

        // the context should not be used at all for non-GET requests
        final HttpContext ctx = mock(HttpContext.class);
        verifyZeroInteractions(ctx);

        for (final Class<? extends HttpRequest> requestClass : requests) {
            final String method = (String) requestClass.getDeclaredField("METHOD_NAME").get(null);
            final HttpRequest req = mock(requestClass);

            final RequestLine reqLine = new BasicRequestLine(method, "", HttpVersion.HTTP_1_1);

            when(req.getRequestLine()).thenReturn(reqLine);
            verifyNoMoreInteractions(req);

            assertThrows(ResumableDownloadIncompatibleRequestException.class, () ->
                    new ResumableDownloadCoordinator(ctx).enhance(req));
        }
    }

    public void refusesToAcceptMoreThanOneETagHint() throws Exception {
        final String etag = new RandomStringGenerator.Builder().withinRange('a', 'z').build().generate(10);
        final ResumableDownloadCoordinator coordinator = new ResumableDownloadCoordinator(mock(HttpContext.class));
        final HttpGet req =
                prepareRequestWithHeaders(unmodifiableMap(IF_MATCH, singleValueHeader(IF_MATCH, etag)));

        // accept an ETag hint
        coordinator.enhance(req);

        assertThrows(ResumableDownloadIncompatibleRequestException.class, () ->
                coordinator.enhance(req));
    }

    public void refusesToAcceptMoreThanOneRangeHint() throws Exception {
        final String range = new HttpRange.Request(0, 1).render();
        final ResumableDownloadCoordinator coordinator = new ResumableDownloadCoordinator(mock(HttpContext.class));
        final HttpGet req =
                prepareRequestWithHeaders(unmodifiableMap(RANGE, singleValueHeader(RANGE, range)));

        // accept a Range hint
        coordinator.enhance(req);

        assertThrows(ResumableDownloadIncompatibleRequestException.class, () ->
                coordinator.enhance(req));
    }

    public void validateResponseExpectsNonNullInputAndStartedState() throws Exception {
        assertThrows(NullPointerException.class, () ->
                new ResumableDownloadCoordinator(mock(HttpContext.class)).validateResponse(null));

        assertThrows(IllegalStateException.class, () ->
                new ResumableDownloadCoordinator(mock(HttpContext.class)).validateResponse(mock(HttpResponse.class)));
    }

    /**
     * Tests validation of responses for downloads with an initial response code 200.
     */
    public void failsWhenResponseLacksRequiredHeaders() throws Exception {
        final String etag = "abc";
        final long contentLength = 2;
        final HttpRange.Request requestRange = new HttpRange.Request(0, contentLength - 1);
        final HttpRange.Response contentRange = new HttpRange.Response(0, contentLength - 1, contentLength);

        final Map<String, Header[]> initial200ResponseHeaders = unmodifiableMap(
                ETAG, singleValueHeader(ETAG, etag),
                CONTENT_LENGTH, singleValueHeader(CONTENT_LENGTH, "2"));

        final Map<String, Header[]> initial206ResponseHeaders = unmodifiableMap(
                ETAG, singleValueHeader(ETAG, etag),
                CONTENT_LENGTH, singleValueHeader(CONTENT_LENGTH, "2"),
                CONTENT_RANGE, singleValueHeader(CONTENT_RANGE, contentRange.render()));

        final HttpResponse everythingMissingResponse = prepareResponseWithHeaders(Collections.EMPTY_MAP);
        final HttpResponse etagOnlyResponse = prepareResponseWithHeaders(unmodifiableMap(
                ETAG, singleValueHeader(ETAG, etag)));
        final HttpResponse rangeOnlyResponse = prepareResponseWithHeaders(unmodifiableMap(
                CONTENT_RANGE, singleValueHeader(CONTENT_RANGE, contentRange.render())));
        final HttpResponse contentLengthOnlyResponse = prepareResponseWithHeaders(unmodifiableMap(
                CONTENT_LENGTH, singleValueHeader(CONTENT_LENGTH, Long.toString(contentLength))));
        final HttpResponse etagAndContentLengthResponse = prepareResponseWithHeaders(unmodifiableMap(
                ETAG, singleValueHeader(ETAG, etag),
                CONTENT_LENGTH, singleValueHeader(CONTENT_LENGTH, Long.toString(contentLength))));

        // started state expects to have enough enough headers to make sure we got a satisfactory response
        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                prepareStartedCoordinator(Collections.EMPTY_MAP, initial200ResponseHeaders).validateResponse(everythingMissingResponse));
        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                prepareStartedCoordinator(Collections.EMPTY_MAP, initial200ResponseHeaders).validateResponse(etagOnlyResponse));
        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                prepareStartedCoordinator(Collections.EMPTY_MAP, initial200ResponseHeaders).validateResponse(contentLengthOnlyResponse));


        // the following validations simulate the initial request being a range request
        // (i.e. _every_ response must have content-range, including the first response)
        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                prepareStartedCoordinator(Collections.EMPTY_MAP, initial206ResponseHeaders).validateResponse(everythingMissingResponse));
        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                prepareStartedCoordinator(Collections.EMPTY_MAP, initial206ResponseHeaders).validateResponse(etagOnlyResponse));
        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                prepareStartedCoordinator(Collections.EMPTY_MAP, initial206ResponseHeaders).validateResponse(rangeOnlyResponse));
        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                prepareStartedCoordinator(Collections.EMPTY_MAP, initial206ResponseHeaders).validateResponse(contentLengthOnlyResponse));
        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                prepareStartedCoordinator(Collections.EMPTY_MAP, initial206ResponseHeaders).validateResponse(etagAndContentLengthResponse));


    }

    public void validatesHintedIfMatch() throws Exception {
        final String etag = new RandomStringGenerator.Builder().withinRange('a', 'z').build().generate(10);

        final Map<String, Header[]> ifMatchHeaders = unmodifiableMap(IF_MATCH, singleValueHeader(IF_MATCH, etag));
        final ResumableDownloadCoordinator ifMatchHintFailingCoordinator = prepareReadyCoordinator(ifMatchHeaders);

        // any returned range is fine since none was specified
        final HttpRange.Response range = new HttpRange.Response(0, 1, 2L);

        // invalid ETag
        final String badEtag = etag.substring(Math.floorDiv(etag.length(), 2));
        final HttpResponse badResponse =
                prepareResponseWithHeaders(unmodifiableMap(ETAG, singleValueHeader(ETAG, badEtag)));

        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                ifMatchHintFailingCoordinator.createMarker(badResponse));

        // build another coordinator expecting the same ETag since the first will have auto-cancelled
        final ResumableDownloadCoordinator ifMatchHintPassingCoordinator = prepareReadyCoordinator(ifMatchHeaders);
        ifMatchHintPassingCoordinator.createMarker(prepareResponseWithHeaders(unmodifiableMap(
                ETAG, singleValueHeader(ETAG, etag),
                CONTENT_LENGTH, singleValueHeader(CONTENT_LENGTH, range.getSize().toString()))));
        assertTrue(ifMatchHintPassingCoordinator.inProgress());
    }

    public void validatesHintedRange() throws Exception {
        final HttpRange.Request range = new HttpRange.Request(0, 1);
        final HttpRange.Response expectedRange = new HttpRange.Response(0, 1, 2);
        final HttpRange.Response badRange = new HttpRange.Response(0, 1, 3);

        final Map<String, Header[]> rangeHeaders = unmodifiableMap(RANGE, singleValueHeader(RANGE, range.render()));
        final ResumableDownloadCoordinator rangeHintFailingCoordinator = prepareReadyCoordinator(rangeHeaders);

        // any returned etag is fine since no if-match was specified
        final String etag = new RandomStringGenerator.Builder().withinRange('a', 'z').build().generate(10);

        // invalid Range
        final HttpResponse badResponse = prepareResponseWithHeaders(unmodifiableMap(
                ETAG, singleValueHeader(ETAG, etag),
                CONTENT_RANGE, singleValueHeader(CONTENT_RANGE, badRange.render())));

        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                rangeHintFailingCoordinator.createMarker(badResponse));

        // build another coordinator expecting the same Range since the first will have auto-cancelled
        final ResumableDownloadCoordinator rangeHintPassingCoordinator = prepareReadyCoordinator(rangeHeaders);
        rangeHintPassingCoordinator.createMarker(prepareResponseWithHeaders(unmodifiableMap(
                ETAG, singleValueHeader(ETAG, etag),
                CONTENT_LENGTH, singleValueHeader(CONTENT_LENGTH, expectedRange.getSize().toString()),
                CONTENT_RANGE, singleValueHeader(CONTENT_RANGE, expectedRange.render()))));
        assertTrue(rangeHintPassingCoordinator.inProgress());
    }


    //     final HttpRange.Response expectedContentRange = new HttpRange.Response(range.getStartInclusive(), range.getEndInclusive(), range.getEndInclusive() + 1);
    // final HttpRange.Response badContentRange = new HttpRange.Response(range.getStartInclusive() + 1, range.getEndInclusive(), range.getEndInclusive() + 1);

    private ResumableDownloadCoordinator prepareReadyCoordinator(final Map<String, Header[]> initialRequestHeaders) throws Exception {
        final HttpGet initialRequest = prepareRequestWithHeaders(initialRequestHeaders);
        final ResumableDownloadCoordinator coordinator = new ResumableDownloadCoordinator(new BasicHttpContext());

        coordinator.enhance(initialRequest);

        return coordinator;
    }

    private ResumableDownloadCoordinator prepareStartedCoordinator(final Map<String, Header[]> initialRequestHeaders,
                                                                   final Map<String, Header[]> initialResponseHeaders) throws Exception {
        final ResumableDownloadCoordinator coordinator = prepareReadyCoordinator(initialRequestHeaders);

        coordinator.createMarker(prepareResponseWithHeaders(initialResponseHeaders));

        return coordinator;
    }

    private static Header[] singleValueHeader(final String name, final String value) {
        return new Header[]{new BasicHeader(name, value)};
    }

    private HttpGet prepareRequestWithHeaders(final Map<String, Header[]> headers) {
        final HttpGet req = prepareMessageWithHeaders(HttpGet.class, headers);

        when(req.getRequestLine()).thenReturn(new BasicRequestLine(HttpGet.METHOD_NAME, "", HttpVersion.HTTP_1_1));

        return req;
    }

    private HttpResponse prepareResponseWithHeaders(final Map<String, Header[]> headers) {
        return prepareMessageWithHeaders(HttpResponse.class, headers);
    }

    private <T extends HttpMessage> T prepareMessageWithHeaders(final Class<T> klass,
                                                                final Map<String, Header[]> headers) {
        final T msg = mock(klass);

        // return an empty list unless a list of headers was provided
        when(msg.getHeaders(anyString())).then(invocation -> {
            final String headerName = invocation.getArgument(0);
            return headers.getOrDefault(headerName, new Header[0]);
        });

        when(msg.getFirstHeader(anyString())).then(invocation -> {
            final String headerName = invocation.getArgument(0);
            if (!headers.containsKey(headerName)) {
                return null;
            }

            final Header[] matched = headers.get(headerName);
            if (matched.length == 0) {
                return null;
            }

            return matched[0];
        });

        for (final Map.Entry<String, Header[]> headerNameAndList : headers.entrySet()) {
            final String headerName = headerNameAndList.getKey();
            when(msg.getHeaders(headerName)).thenReturn(headerNameAndList.getValue());
        }

        return msg;
    }

    public void testBasicFunctionalityWorks() throws Exception {
        final byte[] originalObjectContent = RandomUtils.nextBytes(FIVE_MB);

        final CloseableHttpClient client = prepareClient(
                new FakeHttpClientConnection(
                        new FixedFailureCountByteArrayObjectHttpHandler(originalObjectContent, 2)));

        final HttpUriRequest req = new HttpGet("http://localhost");

        final ByteArrayOutputStream savedEntity = new ByteArrayOutputStream();

        final HttpContext ctx = new HttpClientContext();
        final ResumableDownloadCoordinator coordinator = new ResumableDownloadCoordinator(ctx);

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

            final InputStream entityContent = ent.getContent();
            final ResumableInputStream resumableStream = coordinator.getResumableStream();
            resumableStream.setSource(entityContent);

            try {
                IOUtils.copy(resumableStream, savedEntity, 2);
                finished = true;
            } catch (final IOException e) {
                coordinator.attemptRecovery(e);
                // rethrows fatal exceptions, updates marker otherwise
            } finally {
                res.close();
            }
        } while (!finished && loops < 5);

        if (!finished) {
            throw new AssertionError("Failed to download object content, attempts: " + loops);
        }

        assertArrayEquals("Bytes received do not match original bytes", originalObjectContent, savedEntity.toByteArray());

        LOG.info("leapin' lizards! resumable download test completed successfully with {} attempts!", loops);
    }

    private CloseableHttpClient prepareClient(final HttpClientConnection conn) {
        return HttpClientBuilder.create()
                .setConnectionManager(new FakeHttpClientConnectionManager(conn))
                .setRetryHandler(new StandardHttpRequestRetryHandler())
                .addInterceptorFirst(ResumableDownloadHttpRequestInterceptor.INSTANCE)
                .addInterceptorFirst(ResumableDownloadHttpResponseInterceptor.INSTANCE)
                .build();
    }

}
