/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.UUID;

/**
 * Test class for all-purpose utility methods.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaUtilsTest {

    @Test
    public final void lastCharMatchesWithStringBuilder() {
        char match = '/';
        StringBuilder builder = new StringBuilder("/some/directory/path/");

        boolean actual = MantaUtils.endsWith(builder, match);

        Assert.assertTrue(actual, "Unable to match last character in StringBuilder");
    }

    @Test
    public final void lastCharDoesntMatchWithStringBuilder() {
        char match = '/';
        StringBuilder builder = new StringBuilder("/some/directory/path");

        boolean actual = MantaUtils.endsWith(builder, match);

        Assert.assertFalse(actual, "Matched last character in StringBuilder when we shouldn't have");
    }

    @Test(expectedExceptions = { IllegalArgumentException.class })
    public final void errorOnNullStringBuilder() {
        MantaUtils.endsWith(null, 'c');
    }

    @Test
    public final void doesntMatchOnEmptyStringBuilder() {
        char match = '/';
        StringBuilder builder = new StringBuilder();

        boolean actual = MantaUtils.endsWith(builder, match);

        Assert.assertFalse(actual, "Matched last character in StringBuilder when we shouldn't have because it was empty");
    }

    @Test
    public final void canParseAccountWithNoSubuser() {
        final String account = "username";
        final String[] parts = MantaUtils.parseAccount(account);

        Assert.assertEquals(parts.length, 1,
                "Only a single element should be present");
        Assert.assertEquals(parts[0], account,
                "First element should be username");
    }

    @Test
    public final void canParseAccountWithSubuser() {
        final String account = "username/subuser";
        final String[] parts = MantaUtils.parseAccount(account);

        Assert.assertEquals(parts.length, 2,
                "Both username and subuser should be present");
        Assert.assertEquals(parts[0], "username",
                "Username was not parsed correctly");
        Assert.assertEquals(parts[1], "subuser",
                "Subuser was not parsed correctly");
    }

    @Test(expectedExceptions = { IllegalArgumentException.class })
    public final void wontParseEmptyAccount() {
        final String account = "";
        MantaUtils.parseAccount(account);
    }

    @Test(expectedExceptions = { IllegalArgumentException.class })
    public final void wontParseAccountWithStartingSlash() {
        final String account = "/username/subuser";
        MantaUtils.parseAccount(account);
    }

    @Test(expectedExceptions = { IllegalArgumentException.class })
    public final void wontParseAccountWithTrailingSlash() {
        final String account = "username/";
        MantaUtils.parseAccount(account);
    }

    @Test(expectedExceptions = { IllegalArgumentException.class })
    public final void wontParseAccountWithMiddleAndTrailingSlash() {
        final String account = "username/subuser/";
        MantaUtils.parseAccount(account);
    }

    @Test
    public void lastItemInPathIsCorrectFile() {
        final String expected = new UUID(24, 48).toString();
        final String path = String.format("/foo/bar/%s", expected);

        final String actual = MantaUtils.lastItemInPath(path);

        Assert.assertEquals(actual, expected);
    }

    @Test
    public void lastItemInPathIsCorrectFileWithTrailingSeparator() {
        final String expected = new UUID(24, 48).toString();
        final String path = String.format("/foo/bar/%s/", expected);

        final String actual = MantaUtils.lastItemInPath(path);

        Assert.assertEquals(actual, expected);
    }
}
