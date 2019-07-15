/*
 * Copyright (c) 2018-2019, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.crypto.AesCtrCipherDetails;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.helper.IntegrationTestHelper;
import com.joyent.manta.config.ChainedConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.EncryptionAuthenticationMode;
import com.joyent.manta.config.IntegrationTestConfigContext;
import com.joyent.manta.config.MantaClientMetricConfiguration;
import com.joyent.manta.config.MetricReporterMode;
import com.joyent.test.util.MantaAssert;
import com.joyent.test.util.MantaFunction;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.joyent.manta.exception.MantaErrorCode.RESOURCE_NOT_FOUND_ERROR;
import static java.lang.Math.floorDiv;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * <p>
 * This class encapsulates the situations meant to be remedied by {@link com.joyent.manta.http.ApacheHttpGetResponseEntityContentContinuator}
 * for manual verification.
 * </p>
 * <p>
 * Until either 1. the missing functionality is added to WireMock or 2. Charles Web Proxy allows dynamic request
 * termination through its control panel, integration testing of {@link com.joyent.manta.http.ApacheHttpGetResponseEntityContentContinuator}
 * will require manual user intervention. There is more griping about this (and a Github issue link) in the final
 * paragraph of this JavaDoc.
 * </p>
 * <p>
 * Users are expected to have access to <a href="https://www.charlesproxy.com/">Charles Web Proxy</a> or some other
 * configured proxy designed to cause request failures in a controlled fashion. This test specifically requires turning
 * on throttling so that the download is artificially slowed (giving the operator time to react) and then manually
 * cancelling GET requests in order to trigger the continuator. Warning log messages are written out in a very obvious
 * fashion indicating when each test is about to start and when user intervention is no longer required. If the test
 * operator does not terminate any requests the test will fail when it notices no metrics were recorded.
 * </p>
 * <p>
 * Since we want to make it possible to run this test without a code change this test throws a {@link
 * org.testng.SkipException} if the proxy settings are missing. We expect to find the following settings to not be
 * blank:
 * <ul>
 * <li>http.proxyHost</li>
 * <li>http.proxyPort</li>
 * <li>https.proxyHost</li>
 * <li>https.proxyPort</li>
 * </ul>
 * Assuming the proxy is running locally, these values can be set using the {@code -D} flag when starting the JVM. For
 * example, this test can be invoked with Maven using the following command line:
 * <code>mvn verify -DfailIfNoTests=false -Dtest=ApacheHttpGetResponseEntityContentContinuatorIT
 * -Dhttp.proxyHost=127.0.0.1 -Dhttp.proxyPort=8888 -Dhttps.proxyHost=127.0.0.1 -Dhttps.proxyPort=8888</code> or to run
 * a single test case:
 * <code>mvn verify -DfailIfNoTests=false -Dtest=ApacheHttpGetResponseEntityContentContinuatorIT#regularObjectDownloadUnencrypted
 * -Dhttp.proxyHost=127.0.0.1 -Dhttp.proxyPort=8888 -Dhttps.proxyHost=127.0.0.1 -Dhttps.proxyPort=8888</code>
 * </p>
 * <p>Remember to also pass system properties for client configuration (manta.username/etc.) or set the values in the
 * environment (MANTA_USERNAME/etc).</p>
 * <p>
 * Also note that since several other test cases cover the interactions between {@link
 * com.joyent.manta.util.InputStreamContinuator} and {@link com.joyent.manta.client.crypto.MantaEncryptedObjectInputStream}
 * we only test with a single key strength for each cipher.
 * </p>
 * <p>
 * Recommended throttle preset for Charles Web Proxy: 256K ISDN/DSL
 * </p>
 * <p>
 * Recommended testing procedure:
 * <ul>
 * <li>Start Charles Web Proxy locally</li>
 * <li>Run the test class or an individual test method, making sure to pass the aforementioned system properties</li>
 * <li>As a sanity check, this class uploads and downloads one file for each cipher tested before actually starting any
 * test methods. The use of a "null" cipher means no encryption.</li>
 * <li>Watch the logs for " >>> Pausing for 15s...." which will occur before each test method is run. This gives the
 * tester time to clear the Charles Web Proxy request log and enable throttling.</li>
 * <li>Once the actual request is in flight, (logs show " --- Starting [plain|range] [encrypted|unencrypted] download")
 * the tester should manually terminate the next GET request that shows up in Charles Web Proxy. This can be done as
 * many times as desired.
 * </li>
 * <li>
 * Assuming at least a single request was forcefully terminated, the test case should pass. If the tester forgets to
 * terminate a request, an AssertionError will be thrown when the test sees that no continuation metrics were recorded.
 * </li>
 * </ul>
 * </p>
 * <p>
 * We would ideally automate this with <a href="">WireMock</a> but there is currently
 * <a href="https://github.com/tomakehurst/wiremock/issues/966">no way to configure</a> it to simulate failures
 * exclusively in the response body. Similarly, while Charles Web Proxy allows users to enable a control interface over
 * HTTP it provides no way to terminate in-flight requests.
 * </p>
 */
@Test(groups = {"range-downloads", "buckets"}, singleThreaded = true)
public class ApacheHttpGetResponseEntityContentContinuatorIT {

    private static final Logger LOG = LoggerFactory.getLogger(ApacheHttpGetResponseEntityContentContinuatorIT.class);

    private static final URL STUB_RESOURCE = requireNonNull(Thread.currentThread()
                                                                    .getContextClassLoader()
                                                                    .getResource("Master-Yoda.jpg"));

    private static final byte[] STUB_PLAINTEXT_OBJECT_CONTENT;

    private static final String METRIC_NAME = "get-continuations-recovered-exception-";

    static {
        byte[] plaintextObjectContent = null;
        try {
            plaintextObjectContent = IOUtils.toByteArray(STUB_RESOURCE);
        } catch (final IOException e) {
            LOG.warn("Couldn't load test resource content");
        }

        STUB_PLAINTEXT_OBJECT_CONTENT = plaintextObjectContent;
    }

    private final String testPathPrefix;

    private final ConfigContext dummyConfig;

    private final Map<SupportedCipherDetails, Pair<String, SecretKey>> cipherToObjectAndSecretKey;

    public ApacheHttpGetResponseEntityContentContinuatorIT(final @Optional String testType) throws IOException {
        dummyConfig = new IntegrationTestConfigContext(false);
        final String testName = this.getClass().getSimpleName();
        final MantaClient mantaClient = new MantaClient(dummyConfig);

        testPathPrefix = IntegrationTestHelper.setupTestPath(dummyConfig, mantaClient,
                testName, testType);

        final HashMap<SupportedCipherDetails, Pair<String, SecretKey>> cipherToPathAndKey = new HashMap<>();
        cipherToPathAndKey.put(null,
                               ImmutablePair.of(generatePath(),
                                                null));
        cipherToPathAndKey.put(AesCtrCipherDetails.INSTANCE_128_BIT,
                               ImmutablePair.of(generatePath(),
                                                SecretKeyUtils.generate(AesCtrCipherDetails.INSTANCE_128_BIT)));

        cipherToObjectAndSecretKey = Collections.unmodifiableMap(cipherToPathAndKey);
    }

    @BeforeClass
    @Parameters({"testType"})
    public void prepare(final @Optional String testType) throws IOException {
        // this only needs to be run once but since the constructor shouldn't throw it's placed here
        verifyProxyInUse();

        final MantaClient unencryptedClient = prepareClient(null, null, null);
        final MantaClient encryptedClient = prepareClient(AesCtrCipherDetails.INSTANCE_128_BIT, null, null);

        // we want to upload both encrypted and unencrypted copies of the same file
        // the same parent directory is used for both
        IntegrationTestHelper.createTestBucketOrDirectory(unencryptedClient, testPathPrefix, testType);
        final String unencryptedObjectPath = cipherToObjectAndSecretKey.get(null).getLeft();
        final String encryptedObjectPath = cipherToObjectAndSecretKey.get(AesCtrCipherDetails.INSTANCE_128_BIT).getLeft();
        unencryptedClient.put(unencryptedObjectPath, STUB_PLAINTEXT_OBJECT_CONTENT);
        encryptedClient.put(encryptedObjectPath, STUB_PLAINTEXT_OBJECT_CONTENT);

        unencryptedClient.existsAndIsAccessible(unencryptedObjectPath);
        unencryptedClient.existsAndIsAccessible(encryptedObjectPath);

        assertEquals(IOUtils.toByteArray(unencryptedClient.getAsInputStream(unencryptedObjectPath)),
                     STUB_PLAINTEXT_OBJECT_CONTENT);
        assertEquals(IOUtils.toByteArray(encryptedClient.getAsInputStream(encryptedObjectPath)),
                     STUB_PLAINTEXT_OBJECT_CONTENT);

        // make sure that it's possible to build a new encrypted client and still decrypt the file/metadata
        // (i.e. prepareClient is generating and managing secret keys properly)
        assertEquals(IOUtils.toByteArray(prepareClient(AesCtrCipherDetails.INSTANCE_128_BIT,
                                                       null,
                                                       null).getAsInputStream(encryptedObjectPath)),
                     STUB_PLAINTEXT_OBJECT_CONTENT);
    }

    @BeforeMethod
    public void beforemethod() {
        LOG.warn(" >>> Pausing for 15s to allow tester to enable throttling and prepare to terminate requests");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 15; i++) {
            try {
                sb.append('.');
                LOG.warn(" >>> Paused" + sb.toString());
                TimeUnit.SECONDS.sleep(1);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn(
                        "We were interrupted while waiting for throttling to be enabled, this test class may fail if "
                                + "the human was not ready");
            }
        }
    }

    @AfterClass
    public void teardown() throws IOException {
        LOG.warn(" <<< Finishing download continuation tests. You can stop manually terminating requests now.");

        try (final MantaClient cleanupClient = prepareClient(null, null, null)) {
            IntegrationTestHelper.cleanupTestBucketOrDirectory(cleanupClient, testPathPrefix);
        }
    }

    public void regularObjectDownloadUnencrypted() throws IOException {
        final MetricRegistry metrics = new MetricRegistry();
        final SupportedCipherDetails cipherDetails = null;

        final MantaClient client = prepareClient(cipherDetails, -1, metrics);
        final String unencryptedObjectPath = cipherToObjectAndSecretKey.get(cipherDetails).getLeft();

        final Instant downloadStart = Instant.now();
        LOG.info(" --- Starting plain unencrypted object download of {}", unencryptedObjectPath);
        try (final InputStream in = client.getAsInputStream(unencryptedObjectPath)) {
            assertEquals(IOUtils.toByteArray(in), STUB_PLAINTEXT_OBJECT_CONTENT);
        }
        LOG.info(" --- Finished plain unencrypted download, took: {}s",
                 Duration.between(downloadStart, Instant.now()).getSeconds());

        final Counter exceptions = extractExceptionCounter(metrics);
        assertTrue(0 < exceptions.getCount());

        client.delete(unencryptedObjectPath);
        MantaAssert.assertResponseFailureStatusCode(404, RESOURCE_NOT_FOUND_ERROR,
                (MantaFunction<Object>) () -> client.get(unencryptedObjectPath));
        client.close();
    }

    public void rangeObjectDownloadUnencrypted() throws IOException {
        final MetricRegistry metrics = new MetricRegistry();
        final SupportedCipherDetails cipherDetails = null;

        final MantaClient client = prepareClient(cipherDetails, -1, metrics);
        final String unencryptedObjectPath = cipherToObjectAndSecretKey.get(cipherDetails).getLeft();

        final MantaHttpHeaders headers = new MantaHttpHeaders();

        // start haflway into the file
        final int offset = floorDiv(STUB_PLAINTEXT_OBJECT_CONTENT.length, 2);
        headers.setRange(new HttpRange.UnboundedRequest(offset).render());

        LOG.info(" --- Starting range unencrypted object download of range {} of {}",
                 headers.getRange(),
                 unencryptedObjectPath);
        final byte[] received;
        final Instant downloadStart = Instant.now();
        try (final InputStream in = client.getAsInputStream(unencryptedObjectPath, headers)) {
            received = IOUtils.toByteArray(in);
        }
        LOG.info(" --- Finished plain encrypted download, took: {}s",
                 Duration.between(downloadStart, Instant.now()).getSeconds());

        assertEquals(received,
                     ArrayUtils.subarray(STUB_PLAINTEXT_OBJECT_CONTENT, offset, STUB_PLAINTEXT_OBJECT_CONTENT.length));

        final Counter exceptions = extractExceptionCounter(metrics);
        assertTrue(0 < exceptions.getCount());

        client.delete(unencryptedObjectPath);
        MantaAssert.assertResponseFailureStatusCode(404, RESOURCE_NOT_FOUND_ERROR,
                (MantaFunction<Object>) () -> client.get(unencryptedObjectPath));
        client.close();
    }

    public void regularObjectDownloadEncrypted() throws IOException {
        final MetricRegistry metrics = new MetricRegistry();
        final SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
        final String encryptedObjectPath = cipherToObjectAndSecretKey.get(cipherDetails).getLeft();

        final MantaClient client = prepareClient(cipherDetails, -1, metrics);
        final Instant downloadStart = Instant.now();
        LOG.info(" --- Starting plain encrypted object download of {}", encryptedObjectPath);
        try (final InputStream in = client.getAsInputStream(encryptedObjectPath)) {
            assertEquals(IOUtils.toByteArray(in), STUB_PLAINTEXT_OBJECT_CONTENT);
        }
        LOG.info(" --- Finished plain encrypted download, took: {}s",
                 Duration.between(downloadStart, Instant.now()).getSeconds());

        final Counter exceptions = extractExceptionCounter(metrics);
        assertTrue(0 < exceptions.getCount());

        client.delete(encryptedObjectPath);
        MantaAssert.assertResponseFailureStatusCode(404, RESOURCE_NOT_FOUND_ERROR,
                (MantaFunction<Object>) () -> client.get(encryptedObjectPath));
        client.close();
    }

    public void rangeObjectDownloadEncrypted() throws IOException {
        final MetricRegistry metrics = new MetricRegistry();
        final SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
        final String encryptedObjectPath = cipherToObjectAndSecretKey.get(cipherDetails).getLeft();

        final MantaClient client = prepareClient(cipherDetails, -1, metrics, EncryptionAuthenticationMode.Optional);
        // Avoid "HTTP range requests (random reads) aren't supported when using client-side encryption in mandatory authentication mode."
        // The other tests don't need to set this because they use the HttpClient, not HttpHelper which enforces this

        final MantaHttpHeaders headers = new MantaHttpHeaders();

        // start haflway into the file
        final int offset = floorDiv(STUB_PLAINTEXT_OBJECT_CONTENT.length, 2);
        headers.setRange(new HttpRange.BoundedRequest(offset, STUB_PLAINTEXT_OBJECT_CONTENT.length - 1).render());

        LOG.info(" --- Starting range encrypted object download of range {} of {}",
                 headers.getRange(),
                 encryptedObjectPath);

        final byte[] received;
        final Instant downloadStart = Instant.now();
        try (final InputStream in = client.getAsInputStream(encryptedObjectPath, headers)) {
            received = IOUtils.toByteArray(in);
        }
        LOG.info(" --- Finished range encrypted download, took: {}s",
                 Duration.between(downloadStart, Instant.now()).getSeconds());

        assertEquals(received,
                     ArrayUtils.subarray(STUB_PLAINTEXT_OBJECT_CONTENT, offset, STUB_PLAINTEXT_OBJECT_CONTENT.length));

        client.delete(encryptedObjectPath);
        MantaAssert.assertResponseFailureStatusCode(404, RESOURCE_NOT_FOUND_ERROR,
                (MantaFunction<Object>) () -> client.get(encryptedObjectPath));
        client.close();
    }

    private String generatePath() {
        return String.format("%s%s", this.testPathPrefix, UUID.randomUUID());
    }

    private MantaClient prepareClient(final SupportedCipherDetails cipherDetails,
                                      final Integer continuations,
                                      final MetricRegistry metrics) {
        return prepareClient(cipherDetails, continuations, metrics, EncryptionAuthenticationMode.Mandatory);
    }

    private MantaClient prepareClient(final SupportedCipherDetails cipherDetails,
                                      final Integer continuations,
                                      final MetricRegistry metrics,
                                      final EncryptionAuthenticationMode encryptionAuthenticationMode) {
        // we have to preserve the SecretKey created
        final ChainedConfigContext config = new ChainedConfigContext(dummyConfig);

        if (cipherDetails != null) {
            config.setClientEncryptionEnabled(true);
            config.setEncryptionAlgorithm(cipherDetails.getCipherId());
            config.setEncryptionKeyId("continuator-integration-test-encryption-key");
            final SecretKey secretKey = cipherToObjectAndSecretKey.get(cipherDetails).getRight();
            config.setEncryptionPrivateKeyBytes(secretKey.getEncoded());

            // we could actually
            if (encryptionAuthenticationMode != null) {
                config.setEncryptionAuthenticationMode(encryptionAuthenticationMode);
            } else {
                config.setEncryptionAuthenticationMode(EncryptionAuthenticationMode.Mandatory);
            }

            LOG.info("Secret key used for continuator test (base64, cipher: {}): [{}]",
                     cipherDetails.getCipherId(),
                     Base64.getEncoder().encodeToString(secretKey.getEncoded()));
        }

        if (continuations != null) {
            config.setDownloadContinuations(continuations);
        }

        // just to be safe, explicitly set the buffer size to the 4K default
        config.setHttpBufferSize(DefaultsConfigContext.DEFAULT_HTTP_BUFFER_SIZE);

        final MantaClientMetricConfiguration metricConfig;
        if (metrics != null) {
            metricConfig = new MantaClientMetricConfiguration(UUID.randomUUID(),
                                                              metrics,
                                                              MetricReporterMode.JMX,
                                                              null);
        } else {
            metricConfig = null;
        }

        final MantaClient mantaClient = new MantaClient(config,
                                                        null,
                                                        metricConfig);

        if (!mantaClient.existsAndIsAccessible(config.getMantaHomeDirectory())) {
            Assert.fail("Invalid credentials, cannot proceed with test suite");
        }

        return mantaClient;
    }

    private static void verifyProxyInUse() {
        List<String> proxyProps = Arrays.asList(
                "http.proxyHost",
                "http.proxyPort",
                "https.proxyHost",
                "https.proxyPort");

        ArrayList<String> missingProps = new ArrayList<>();

        for (String prop : proxyProps) {
            if (null == System.getProperty(prop)) {
                missingProps.add(prop);
            }
        }

        if (missingProps.isEmpty()) {
            return;
        }

        final String message =
                "Skipping ApacheHttpGetResponseEntityContentContinuatorIT because proxy settings were missing: "
                        + StringUtils.join(missingProps);

        LOG.warn(message);

        throw new SkipException(message);
    }

    private static final MetricFilter METRIC_FILTER_CONTINUATIONS_HISTOGRAM = MetricFilter.startsWith(METRIC_NAME);

    private static Counter extractExceptionCounter(final MetricRegistry metrics) {
        final SortedMap<String, Counter> counters = metrics.getCounters(METRIC_FILTER_CONTINUATIONS_HISTOGRAM);

        if (counters.isEmpty()) {
            fail("No continuations were recorded!");
        }

        // we can grab any relevant counter, they are never created without also being incremented
        final Counter counter = counters.get(counters.firstKey());

        if (counter == null) {
            fail("No continuations were recorded (though it seemed like there would be)!");
        }

        return counter;
    }
}
