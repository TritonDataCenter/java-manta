/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

@Test
public class MultipartOutputStreamTest {

    public void happyPath() throws Exception {
        ByteArrayOutputStream s1 = new ByteArrayOutputStream();
        ByteArrayOutputStream s2 = new ByteArrayOutputStream();
        ByteArrayOutputStream s3 = new ByteArrayOutputStream();
        MultipartOutputStream mpos = new MultipartOutputStream(16);

        mpos.setNext(s1);
        mpos.write("foo".getBytes("UTF-8"));
        mpos.write("foo".getBytes("UTF-8"));
        Assert.assertEquals(s1.toString("UTF-8"), "");
        mpos.flushBuffer();

        mpos.setNext(s2);
        mpos.write("bar".getBytes("UTF-8"));
        mpos.flushBuffer();

        mpos.setNext(s3);
        mpos.write("baz".getBytes("UTF-8"));
        mpos.flushBuffer();

        Assert.assertEquals(s1.toString("UTF-8"), "foofoo");
        Assert.assertEquals(s2.toString("UTF-8"), "bar");
        Assert.assertEquals(s3.toString("UTF-8"), "baz");
    }

    public void bufSwitchOut() throws Exception {
        ByteArrayOutputStream s1 = new ByteArrayOutputStream();
        ByteArrayOutputStream s2 = new ByteArrayOutputStream();
        MultipartOutputStream mpos = new MultipartOutputStream(16);

        mpos.setNext(s1);
        mpos.write("foo".getBytes(StandardCharsets.UTF_8));
        mpos.setNext(s2);
        mpos.flushBuffer();
        Assert.assertEquals(s1.toString("UTF-8"), "");
        Assert.assertEquals(s2.toString("UTF-8"), "foo");
    }

    public void allAligned() throws Exception {
        ByteArrayOutputStream s1 = new ByteArrayOutputStream();
        ByteArrayOutputStream s2 = new ByteArrayOutputStream();
        MultipartOutputStream mpos = new MultipartOutputStream(4);

        mpos.setNext(s1);
        mpos.write("fooo".getBytes(StandardCharsets.UTF_8));
        mpos.write("baarbaar".getBytes(StandardCharsets.UTF_8));
        mpos.setNext(s2);

        Assert.assertEquals(s1.toString("UTF-8"), "fooobaarbaar");
        Assert.assertEquals(s2.toString("UTF-8"), "");

    }

    public void partRemainder() throws Exception {
        ByteArrayOutputStream s1 = new ByteArrayOutputStream();
        ByteArrayOutputStream s2 = new ByteArrayOutputStream();
        ByteArrayOutputStream s3 = new ByteArrayOutputStream();
        ByteArrayOutputStream s4 = new ByteArrayOutputStream();
        ByteArrayOutputStream s5 = new ByteArrayOutputStream();
        MultipartOutputStream mpos = new MultipartOutputStream(16);

        mpos.setNext(s1);
        mpos.write("Hello ".getBytes(StandardCharsets.UTF_8));
        mpos.setNext(s2);
        mpos.write("world ".getBytes(StandardCharsets.UTF_8));
        mpos.setNext(s3);
        mpos.write("Joyent".getBytes(StandardCharsets.UTF_8));
        mpos.setNext(s4);
        mpos.write("!".getBytes(StandardCharsets.UTF_8));
        s5.write(mpos.getRemainder());

        Assert.assertEquals(s1.toString("UTF-8"), "");
        Assert.assertEquals(s2.toString("UTF-8"), "");
        Assert.assertEquals(s3.toString("UTF-8"), "Hello world Joye");
        Assert.assertEquals(s4.toString("UTF-8"), "");
        Assert.assertEquals(s5.toString("UTF-8"), "nt!");
    }
}
