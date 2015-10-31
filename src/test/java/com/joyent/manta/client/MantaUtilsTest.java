/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import org.testng.Assert;
import org.testng.annotations.Test;

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
}
