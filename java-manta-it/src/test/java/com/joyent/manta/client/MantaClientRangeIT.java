package com.joyent.manta.client;

import com.joyent.manta.config.BaseChainedConfigContext;
import com.joyent.manta.config.EncryptionAuthenticationMode;
import com.joyent.manta.config.IntegrationTestConfigContext;
import com.joyent.manta.config.SettableConfigContext;
import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
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
    private static final String TEST_DATA =
            "A SERGEANT OF THE LAW, wary and wise, " +
            "That often had y-been at the Parvis, <26> " +
            "There was also, full rich of excellence. " +
            "Discreet he was, and of great reverence: " +
            "He seemed such, his wordes were so wise, " +
            "Justice he was full often in assize, " +
            "By patent, and by plein* commission; " +
            "For his science, and for his high renown, " +
            "Of fees and robes had he many one. " +
            "So great a purchaser was nowhere none. " +
            "All was fee simple to him, in effect " +
            "His purchasing might not be in suspect* " +
            "Nowhere so busy a man as he there was " +
            "And yet he seemed busier than he was " +
            "In termes had he case' and doomes* all " +
            "That from the time of King Will. were fall. " +
            "Thereto he could indite, and make a thing " +
            "There coulde no wight *pinch at* his writing. " +
            "And every statute coud* he plain by rote " +
            "He rode but homely in a medley* coat, " +
            "Girt with a seint* of silk, with barres small; " +
            "Of his array tell I no longer tale.";

    private MantaClient mantaClient;

    private String testPathPrefix;

    @BeforeClass
    @Parameters({"usingEncryption"})
    public void beforeClass(@Optional Boolean usingEncryption) throws IOException {

        // Let TestNG configuration take precedence over environment variables
        SettableConfigContext<BaseChainedConfigContext> config = new IntegrationTestConfigContext(usingEncryption);

        // Range request have to be in optional authentication mode
        if (config.isClientEncryptionEnabled()) {
            config.setEncryptionAuthenticationMode(EncryptionAuthenticationMode.Optional);
        }

        mantaClient = new MantaClient(config);
        testPathPrefix = String.format("%s/stor/java-manta-integration-tests/%s",
                config.getMantaHomeDirectory(), UUID.randomUUID());
        mantaClient.putDirectory(testPathPrefix, true);
    }

    public final void canGetWithRangeHeader() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final String expected = TEST_DATA.substring(7, 18); // substring is inclusive, exclusive

        mantaClient.put(path, TEST_DATA);

        final MantaHttpHeaders headers = new MantaHttpHeaders();
        // Range is inclusive, inclusive
        headers.setRange("bytes=7-17");

        try (final InputStream min = mantaClient.getAsInputStream(path, headers)) {
            String actual = IOUtils.toString(min, Charset.defaultCharset());
            Assert.assertEquals(actual, expected, "Didn't receive correct range value");
        }
    }

    public final void canGetWithComputedRangeHeader() throws IOException {
        // see testCanGetWithRangeHeader above
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final long startPos = 7;
        final long endPos = 49;
        final String expected = TEST_DATA.substring((int)startPos, (int)endPos + 1); // substring is inclusive, exclusive
        mantaClient.put(path, TEST_DATA);

        final MantaHttpHeaders headers = new MantaHttpHeaders();
        try (final InputStream min = mantaClient.getAsInputStream(path, headers, startPos, endPos)) {
            String actual = IOUtils.toString(min, Charset.defaultCharset());
            Assert.assertEquals(actual, expected, "Didn't receive correct range value");
        }
    }

    public final void canGetWithUnboundedEndRange() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final String expected = StringUtils.substring(TEST_DATA, 50);

        mantaClient.put(path, TEST_DATA);

        final MantaHttpHeaders headers = new MantaHttpHeaders();
        // Range is inclusive, inclusive
        headers.setRange("bytes=50-");

        try (final InputStream min = mantaClient.getAsInputStream(path, headers)) {
            String actual = IOUtils.toString(min, Charset.defaultCharset());
            Assert.assertEquals(actual, expected, "Didn't receive correct range value");
        }
    }

    public final void canGetWithUnboundedStartRange() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final String expected = StringUtils.substring(TEST_DATA, -50);

        mantaClient.put(path, TEST_DATA);

        final MantaHttpHeaders headers = new MantaHttpHeaders();
        // Range is inclusive, inclusive
        headers.setRange("bytes=-50");

        try (final InputStream min = mantaClient.getAsInputStream(path, headers)) {
            String actual = IOUtils.toString(min, Charset.defaultCharset());
            Assert.assertEquals(actual, expected, "Didn't receive correct range value");
        }
    }


    public final void canGetWithEndRangeBeyondObjectSize() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final String expected = StringUtils.substring(TEST_DATA, 50);

        mantaClient.put(path, TEST_DATA);

        final MantaHttpHeaders headers = new MantaHttpHeaders();
        // Range is inclusive, inclusive
        headers.setRange("bytes=50-" + Integer.MAX_VALUE);

        try (final InputStream min = mantaClient.getAsInputStream(path, headers)) {
            String actual = IOUtils.toString(min, Charset.defaultCharset());
            Assert.assertEquals(actual, expected, "Didn't receive correct range value");
        }
    }

    public final void canGetWithZeroRange() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final String expected = StringUtils.substring(TEST_DATA, 0, 1);

        mantaClient.put(path, TEST_DATA);

        final MantaHttpHeaders headers = new MantaHttpHeaders();
        // Range is inclusive, inclusive
        headers.setRange("bytes=0-0");

        try (final InputStream min = mantaClient.getAsInputStream(path, headers)) {
            String actual = IOUtils.toString(min, Charset.defaultCharset());
            Assert.assertEquals(actual, expected, "Didn't receive correct range value");
        }
    }

    @Test (enabled = false)
    public final void canGetAllRanges() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        mantaClient.put(path, TEST_DATA);

        for (int start = 0; start < TEST_DATA.length(); start++) {
            for (int end = 0; end < TEST_DATA.length(); end++) {
                String expected = StringUtils.substring(TEST_DATA, start, end + 1);

                final MantaHttpHeaders headers = new MantaHttpHeaders();
                // Range is inclusive, inclusive
                String rangeHeader = "bytes=" + start + "-" + end;
                headers.setRange(rangeHeader);

                try (final InputStream min = mantaClient.getAsInputStream(path, headers)) {
                    String actual = IOUtils.toString(min, Charset.defaultCharset());
                    Assert.assertEquals(actual, expected, "Didn't receive correct range value for range: " + rangeHeader);
                }
            }
        }
    }
}
