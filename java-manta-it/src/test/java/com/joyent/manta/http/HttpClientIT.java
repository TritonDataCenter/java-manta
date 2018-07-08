package com.joyent.manta.http;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.ResponseTransformer;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.Response;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.config.IntegrationTestConfigContext;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.ProxyInputStream;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.SocketConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static java.lang.Math.round;
import static java.lang.System.nanoTime;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class HttpClientIT {

    private static final Logger LOG = LoggerFactory.getLogger(HttpClientIT.class);

    private WireMockServer wireMockServer;

    private MantaClient mantaClient;

    private static class IncreasingDelayInputStream extends ProxyInputStream {

        private final int baseDelay;

        private final float mult;

        private int n;

        IncreasingDelayInputStream(final InputStream proxy,
                                   final int baseDelay,
                                   final float mult) {
            super(proxy);
            this.baseDelay = baseDelay;
            this.mult = mult;
            this.n = 0;
        }

        @Override
        protected void beforeRead(final int n) {
            try {
                final int sleeping = round(this.n++ * this.baseDelay * this.mult);
                LOG.info(" > sleeping " + sleeping);
                Thread.sleep(sleeping);
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }

    }

    private static class IncreasingDelayReadingBodyResponse extends Response {
        IncreasingDelayReadingBodyResponse(final Response response,
                                           final int baseDelay,
                                           final float mult) {
            super(response.getStatus(),
                  response.getStatusMessage(),
                  () -> new IncreasingDelayInputStream(response.getBodyStream(), baseDelay, mult),
                  response.getHeaders(),
                  response.wasConfigured(),
                  response.getFault(),
                  response.getInitialDelay(),
                  response.getChunkedDribbleDelay(),
                  response.isFromProxy());
        }
    }

    @BeforeClass
    @org.testng.annotations.Parameters({"usingEncryption"})
    public void setup(@Optional final Boolean usingEncryption) {

        final WireMockConfiguration wmc = wireMockConfig()
                .dynamicPort()
                .disableRequestJournal()
                .extensions(new ResponseTransformer() {
                    @Override
                    public Response transform(final Request request,
                                              final Response response,
                                              final FileSource files,
                                              final Parameters parameters) {
                        return new IncreasingDelayReadingBodyResponse(response, 200, 1.1f);
                    }

                    @Override
                    public String getName() {
                        return "increasing-delay";
                    }
                });
        ;

        this.wireMockServer = new WireMockServer(wmc);
        this.wireMockServer.start();

        assertTrue(this.wireMockServer.isRunning());

        // build a config context so we can figure out where to proxy to
        final IntegrationTestConfigContext config =
                new IntegrationTestConfigContext(usingEncryption);
        //
        // stubFor(get(urlMatching(".*"))
        //                 .willReturn(aResponse()
        //                                     .proxiedFrom(config.getMantaURL())
        //                                     .withChunkedDribbleDelay(2, 1000)
        //                             // .withFault()
        //                 ));

        // wiremock stubbing configuration
        configureFor(this.wireMockServer.port());

        // global delays will be applied before chunked-dribble delay
        // setGlobalFixedDelay(00);
        // setGlobalRandomDelay(new UniformDistribution(1000, 5000));

        // the amount of time specified to ChunkedDribbleDelay seems to be
        // divided evenly across the entire HTTP response, including the headers
        stubFor(get(urlMatching(".*"))
                        .willReturn(aResponse()
                                            .proxiedFrom("https://postman-echo.com/get")
                        ));

        // point MantaClient at localhost proxy
        config.setMantaURL(this.wireMockServer.url(""));
        config.setTimeout(500);

        System.out.println(String.format("manta url: [%s]", config.getMantaURL()));

        this.mantaClient = new MantaClient(config);
    }

    @AfterClass
    public void teardown() {
        this.wireMockServer.stop();
    }

    public void testFailure() throws Exception {

        final String object = this.wireMockServer.url("?foo=bar");
        // final String object = this.wireMockServer.url("/public/file.jpg");

        final HttpGet get = new HttpGet(object);
        final long start = nanoTime();

        int soTimeout = 1000;

        try (final CloseableHttpClient client = HttpClients.custom()
                .setDefaultSocketConfig(
                        SocketConfig.custom()
                                .setSoTimeout(soTimeout)
                                .build())
                .setDefaultRequestConfig(
                        RequestConfig.custom()
                                .setSocketTimeout(soTimeout)
                                .build())
                .setKeepAliveStrategy(DefaultConnectionKeepAliveStrategy.INSTANCE)
                .build()) {
            try (final CloseableHttpResponse response = client.execute(get)) {

                timedLog(start, "read header");
                LOG.info("response line: " + response.getStatusLine());
                LOG.info("response body (streamed):");

                final HttpEntity entity = response.getEntity();

                if (true) {
                    final byte[] buf = new byte[16];
                    try (final InputStream body = entity.getContent()) {
                        int n = 0;
                        long readStart = nanoTime();
                        while (((n = IOUtils.read(body, buf)) == buf.length)) {
                            timedLog(readStart, new String(buf, StandardCharsets.US_ASCII));
                            readStart = nanoTime();
                        }
                        timedLog(readStart, new String(Arrays.copyOfRange(buf, 0, n), StandardCharsets.US_ASCII));
                    }

                } else if (false) {
                    try (final InputStream body = entity.getContent()) {
                        final StringWriter sw = new StringWriter();
                        IOUtils.copy(body, sw, StandardCharsets.US_ASCII);
                        LOG.warn(" >> " + sw.toString());
                    }

                } else {
                    LOG.warn(" >> " + EntityUtils.toString(entity));
                }

                LOG.warn("runtime: {} ms", (nanoTime() - start) / 1_000_000);
                // final byte[] buf = new byte[16];
                // try (final InputStream body = entity.getContent()) {
                //     int n = 0;
                //     while (buf.length > IOUtils.read(body, buf)) {
                //         LOG.info("chunk: " + IOUtils.toString(buf, StandardCharsets.US_ASCII.name()));
                //     }
                // }
            }

            System.out.println("closing client");

        }
    }

    public static void timedLog(final long startNanos, final String chunk) {
        LOG.warn(
                String.format(
                        " >> (%4d ms) >> %s",
                        (nanoTime() - startNanos) / 1_000_000,
                        chunk));
    }
}
