package com.joyent.manta.client.multipart;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;

@Test
public class MultipartOutputStreamTest {

    public void happyPath() throws Exception {
        ByteArrayOutputStream s1 = new ByteArrayOutputStream();
        ByteArrayOutputStream s2 = new ByteArrayOutputStream();
        ByteArrayOutputStream s3 = new ByteArrayOutputStream();
        MultipartOutputStream mpos = new MultipartOutputStream(s1);
        mpos.write("foo".getBytes("UTF-8"));
        mpos.write("foo".getBytes("UTF-8"));
        mpos.setNext(s2);
        mpos.write("bar".getBytes("UTF-8"));
        mpos.setNext(s3);
        mpos.write("baz".getBytes("UTF-8"));

        Assert.assertEquals(s1.toString("UTF-8"), "foofoo");
        Assert.assertEquals(s2.toString("UTF-8"), "bar");
        Assert.assertEquals(s3.toString("UTF-8"), "baz");

    }

}
