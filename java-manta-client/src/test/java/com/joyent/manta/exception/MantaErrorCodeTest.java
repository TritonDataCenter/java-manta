package com.joyent.manta.exception;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests the behavior of the {@link MantaErrorCode} enumeration.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaErrorCodeTest {
    @Test
    public void valueOfCodeCanFindByMatchingCode() {
        final MantaErrorCode expected = MantaErrorCode.INVALID_LIMIT_ERROR;
        final MantaErrorCode actual = MantaErrorCode.valueOfCode("InvalidLimit");

        Assert.assertEquals(actual, expected);
    }

    @Test
    public void valueOfCodeCanFindByNonmatching() {
        final MantaErrorCode expected = MantaErrorCode.UNKNOWN_ERROR;
        final MantaErrorCode actual = MantaErrorCode.valueOfCode("Who knows?");

        Assert.assertEquals(actual, expected);
    }

    @Test
    public void valueOfCodeCanFindByNull() {
        final MantaErrorCode expected = MantaErrorCode.NO_CODE_ERROR;
        final MantaErrorCode actual = MantaErrorCode.valueOfCode(null);

        Assert.assertEquals(actual, expected);
    }
}
