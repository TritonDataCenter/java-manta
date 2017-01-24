package com.joyent.manta.http;

import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class MantaHttpHeadersByteRangeTest {

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void dualNullCheck() {
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setByteRange(null, null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void negativeStartCheck() {
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setByteRange(-7L, null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void negativeEndCheck() {
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setByteRange(null, 0L);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void cartBeforeHorse() {
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setByteRange(21L, 13L);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void negativeStart() {
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setByteRange(-2L, 2L);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void negativeEnd() {
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setByteRange(-22L, -2L);
    }

    public void happyPath() {
        MantaHttpHeaders headers = new MantaHttpHeaders();
        Assert.assertEquals(headers.setByteRange(0L, 11L).getRange(),
                            "bytes=0-11");
        Assert.assertEquals(headers.setByteRange(100L, 9999L).getRange(),
                            "bytes=100-9999");
        Assert.assertEquals(headers.setByteRange(50L, null).getRange(),
                            "bytes=50-");
        Assert.assertEquals(headers.setByteRange(null, 137L).getRange(),
                            "bytes=-137");
        // RFC 7233 Examples
        Assert.assertEquals(headers.setByteRange(0L, 499L).getRange(),
                            "bytes=0-499");
        Assert.assertEquals(headers.setByteRange(500L, 999L).getRange(),
                            "bytes=500-999");
        Assert.assertEquals(headers.setByteRange(null, 500L).getRange(),
                            "bytes=-500");
        Assert.assertEquals(headers.setByteRange(9500L, null).getRange(),
                            "bytes=9500-");
        
    }
}
