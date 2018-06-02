package com.joyent.manta.http;

import com.joyent.manta.util.ResumableInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpEntity;
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
import java.io.InputStream;

import static com.joyent.manta.http.FixedFailureCountCharacterRepeatingHttpHandler.HEADER_INPUT_CHARACTER;
import static com.joyent.manta.http.FixedFailureCountCharacterRepeatingHttpHandler.HEADER_REPEAT_COUNT;
import static java.lang.System.out;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.http.HttpStatus.SC_PARTIAL_CONTENT;
import static org.testng.Assert.assertEquals;

@Test
public class ResumableDownloadCoordinatorTest {

    private static final Logger LOG = LoggerFactory.getLogger(ResumableDownloadCoordinatorTest.class);

    public void testBasicFunctionalityWorks() throws Exception {

        final CloseableHttpClient client = prepareClient(
                new FakeHttpClientConnection(
                        new FixedFailureCountCharacterRepeatingHttpHandler(2)));

        final HttpUriRequest req = new HttpGet("http://localhost");

        final int expectedEntityLength = 16;
        final String inputChar = "a";
        req.setHeader(HEADER_REPEAT_COUNT, Integer.toString(expectedEntityLength));
        req.setHeader(HEADER_INPUT_CHARACTER, inputChar);

        final ByteArrayOutputStream savedEntity = new ByteArrayOutputStream();

        final HttpContext ctx = new HttpClientContext();
        final ResumableInputStream resumableStream = new ResumableInputStream(2);
        final ResumableDownloadCoordinator coordinator = new ResumableDownloadCoordinator(resumableStream, ctx);

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
            resumableStream.setSource(entityContent);

            try {
                IOUtils.copy(resumableStream, savedEntity, 2);
                finished = true;
            } catch (final IOException e) {
                if (ResumableDownloadCoordinator.isRecoverable(e)) {
                    coordinator.updateMarkerFromStream();
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


        final String expectedEntity = StringUtils.repeat(inputChar, expectedEntityLength);
        assertEquals(expectedEntity, new String(savedEntity.toByteArray(), US_ASCII));

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
