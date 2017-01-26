package com.joyent.manta.client;

import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.UUID;

@Test
public class MantaClientRangeIT {

    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";

    private MantaClient mantaClient;

    private String testPathPrefix;

    @BeforeClass
    @Parameters({"usingEncryption"})
    public void beforeClass(@Optional Boolean usingEncryption) throws IOException {

        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new IntegrationTestConfigContext(usingEncryption);

        mantaClient = new MantaClient(config);
        testPathPrefix = String.format("%s/stor/java-manta-integration-tests/%s",
                config.getMantaHomeDirectory(), UUID.randomUUID());
        mantaClient.putDirectory(testPathPrefix, true);
    }

    public final void testCanGetWithRangeHeader() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final String expected = TEST_DATA.substring(7, 18); // substring is inclusive, exclusive

        // Test data: "EPISODEII_IS_BEST_EPISODE"
        // Our Range:         [---------]

        mantaClient.put(path, TEST_DATA);

        final MantaHttpHeaders headers = new MantaHttpHeaders();
        // Range is inclusive, inclusive
        headers.setRange("bytes=7-17");

        try (final InputStream min = mantaClient.getAsInputStream(path, headers)) {
            String actual = IOUtils.toString(min, Charset.defaultCharset());
            Assert.assertEquals(actual, expected, "Didn't receive correct range value");
        }
    }

    public final void testCanGetWithComputedRangeHeader() throws IOException {
        // see testCanGetWithRangeHeader above
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final String expected = TEST_DATA.substring(7, 18); // substring is inclusive, exclusive
        mantaClient.put(path, TEST_DATA);

        final MantaHttpHeaders headers = new MantaHttpHeaders();
        try (final InputStream min = mantaClient.getAsInputStream(path, headers, 7L, 17L)) {
            String actual = IOUtils.toString(min, Charset.defaultCharset());
            Assert.assertEquals(actual, expected, "Didn't receive correct range value");
        }
    }
}
