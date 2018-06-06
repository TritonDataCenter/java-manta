package com.joyent.manta.http;

import com.joyent.manta.exception.ResumableDownloadIncompatibleRequestException;
import com.joyent.manta.util.ResumableInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.http.Header;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpEntity;
import org.apache.http.HttpVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
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
import java.util.Map;

import static com.joyent.manta.util.MantaUtils.unmodifiableMap;
import static java.lang.System.out;
import static java.util.Collections.emptyMap;
import static org.apache.http.HttpHeaders.IF_MATCH;
import static org.apache.http.HttpHeaders.RANGE;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.http.HttpStatus.SC_PARTIAL_CONTENT;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
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

    public void coordinatorWithMarkerIsInProgress() throws Exception {
        final ResumableDownloadCoordinator coordinator = new ResumableDownloadCoordinator(mock(HttpContext.class));

        assertFalse(coordinator.inProgress());

        coordinator.createMarker(mock(ResumableDownloadMarker.class));

        assertTrue(coordinator.inProgress());
    }

    public void validatesNoCollidingHeadersForInitialRequest() throws Exception {
        validatesCompatibleHeadersForInitialRequest(emptyMap());
    }

    public void validatesCompatibleIfMatchHeaderForInitialRequest() throws Exception {
        final String ifMatchHeaderValue = new HttpRange.Request(0, 10).renderRequestRange();

        // users are allowed to omit or set their own if-match header
        validatesCompatibleHeadersForInitialRequest(
                unmodifiableMap(IF_MATCH, new Header[]{}));
        validatesCompatibleHeadersForInitialRequest(
                unmodifiableMap(IF_MATCH, new Header[]{new BasicHeader(IF_MATCH, ifMatchHeaderValue)}));

        // this should never occur so we consider it programmer error, something has been subclassed incorrectly
        assertThrows(ResumableDownloadIncompatibleRequestException.class, () -> {
            validatesCompatibleHeadersForInitialRequest(
                    unmodifiableMap(IF_MATCH, new Header[]{null}));
        });

        // we make no effort to merge headers
        assertThrows(ResumableDownloadIncompatibleRequestException.class, () -> {
            validatesCompatibleHeadersForInitialRequest(
                    unmodifiableMap(IF_MATCH, new Header[]{new BasicHeader(IF_MATCH, ifMatchHeaderValue), new BasicHeader(IF_MATCH, ifMatchHeaderValue)}));
        });
    }

    public void validatesCompatibleRangeHeaderForInitialRequest() throws Exception {
        final String rangeHeaderValue = new HttpRange.Request(0, 10).renderRequestRange();

        // users are allowed to omit or set their own initial range header
        validatesCompatibleHeadersForInitialRequest(
                unmodifiableMap(RANGE, new Header[]{}));
        validatesCompatibleHeadersForInitialRequest(
                unmodifiableMap(RANGE, new Header[]{new BasicHeader(RANGE, rangeHeaderValue)}));

        // this should never occur so we consider it programmer error, something has been subclassed incorrectly
        assertThrows(ResumableDownloadIncompatibleRequestException.class, () -> {
            validatesCompatibleHeadersForInitialRequest(
                    unmodifiableMap(RANGE, new Header[]{null}));
        });

        // we explicitly do not handle resuming multiple ranges (though we could in the future)
        // (supplying the same range header twice is also an error, but we're not concerned with that)
        assertThrows(ResumableDownloadIncompatibleRequestException.class, () -> {
            validatesCompatibleHeadersForInitialRequest(
                    unmodifiableMap(RANGE, new Header[]{new BasicHeader(RANGE, rangeHeaderValue), new BasicHeader(RANGE, rangeHeaderValue)}));
        });
    }

    private void validatesCompatibleHeadersForInitialRequest(final Map<String, Header[]> userSuppliedHeaders) throws Exception {
        final HttpGet req = mock(HttpGet.class);
        when(req.getRequestLine()).thenReturn(new BasicRequestLine(HttpGet.METHOD_NAME, "", HttpVersion.HTTP_1_1));

        // return an empty list unless a list of headers was provided
        when(req.getHeaders(anyString())).then(invocation -> {
            final String headerName = invocation.getArgument(0);
            if (userSuppliedHeaders.containsKey(headerName)) {
                return userSuppliedHeaders.get(headerName);
            }
            return new Header[0];
        });

        for (final Map.Entry<String, Header[]> headers : userSuppliedHeaders.entrySet()) {
            final String headerName = headers.getKey();
            when(req.getHeaders(headerName)).thenReturn(headers.getValue());
        }
        verifyNoMoreInteractions(req);

        final HttpContext ctx = new BasicHttpContext();

        new ResumableDownloadCoordinator(ctx).validateExistingRequestHeaders(req);
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
