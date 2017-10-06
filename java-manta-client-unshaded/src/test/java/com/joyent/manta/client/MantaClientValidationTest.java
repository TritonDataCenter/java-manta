/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.StandardConfigContext;
import com.joyent.manta.config.SystemSettingsConfigContext;
import com.joyent.manta.util.UnitTestConstants;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.File;


@Test
public class MantaClientValidationTest {

    protected MantaClient client;

    protected MantaClient buildClient(final ConfigContext config) {
        return new MantaClient(config);
    }

    protected MantaClient makeTestClient() {
        final ConfigContext overwrite = new StandardConfigContext()
                .setMantaUser("phantomtest")
                .setMantaKeyId(UnitTestConstants.FINGERPRINT)
                .setPrivateKeyContent(UnitTestConstants.PRIVATE_KEY);
        final ConfigContext config = new SystemSettingsConfigContext(overwrite);

        try {
            System.setProperty("manta.dontValidateConfig", "true");
            return buildClient(config);
        } finally {
            System.setProperty("manta.dontValidateConfig", "false");
        }
    }

    @BeforeClass
    public void beforeClass() {
        client = makeTestClient();
    }

    public void deleteTest() throws Exception {
        Assert.assertThrows(NullPointerException.class, () -> {
                client.delete(null);
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.delete("");
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.delete(" ");
            });
    }

    public void getTest() throws Exception {
        Assert.assertThrows(NullPointerException.class, () -> {
                client.get(null);
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.get("");
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.get(" ");
            });
    }

    public void getAsInputStreamTest() throws Exception {
        Assert.assertThrows(NullPointerException.class, () -> {
                client.getAsInputStream(null);
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.getAsInputStream("");
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.getAsInputStream(" ");
            });
    }

    public void getSeekableByteChannelTest() throws Exception {
        Assert.assertThrows(NullPointerException.class, () -> {
                client.getSeekableByteChannel(null);
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.getSeekableByteChannel("");
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.getSeekableByteChannel(" ");
            });
        Assert.assertThrows(NullPointerException.class, () -> {
                client.getSeekableByteChannel(null, 137);
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.getSeekableByteChannel("", 137);
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.getSeekableByteChannel(" ", 137);
            });
    }


    public void getAsSignedURITest() throws Exception {
        Assert.assertThrows(NullPointerException.class, () -> {
                client.getAsSignedURI(null, "GET", 1000000000);
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.getAsSignedURI("", "GET", 1000000000);
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.getAsSignedURI(" ", "GET", 1000000000);
            });
    }

    public void headTest() throws Exception {
        Assert.assertThrows(NullPointerException.class, () -> {
                client.head(null);
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.head("");
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.head(" ");
            });
    }

    public void streamingIteratorTest() throws Exception {
        Assert.assertThrows(NullPointerException.class, () -> {
                client.streamingIterator(null);
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.streamingIterator("");
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.streamingIterator(" ");
            });
    }

    public void putTest() throws Exception {
        Assert.assertThrows(NullPointerException.class, () -> {
                client.put(null, new ByteArrayInputStream(new byte[137]));
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.put("", new ByteArrayInputStream(new byte[137]));
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.put(" ", new ByteArrayInputStream(new byte[137]));
            });
    }

    public void putAsOutputStreamTest() throws Exception {
        Assert.assertThrows(NullPointerException.class, () -> {
                client.putAsOutputStream(null);
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.putAsOutputStream("");
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.putAsOutputStream(" ");
            });
    }

    public void putStringTest() throws Exception {
        Assert.assertThrows(NullPointerException.class, () -> {
                client.put(null, "foo");
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.put("", "foo");
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.put(" ", "foo");
            });
    }

    public void putArrayTest() throws Exception {
        Assert.assertThrows(NullPointerException.class, () -> {
                client.put(null, new byte[137]);
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.put("", new byte[137]);
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.put(" ", new byte[137]);
            });
    }

    public void putFileTest() throws Exception {
        Assert.assertThrows(NullPointerException.class, () -> {
                client.put(null, new File("does-not-exist"));
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.put("", new File("does-not-exist"));
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.put(" ", new File("does-not-exist"));
            });
    }

    public void putMetadataTest() throws Exception {
        MantaMetadata md = new MantaMetadata();
        md.put("m-hello", "world");
        Assert.assertThrows(NullPointerException.class, () -> {
                client.putMetadata(null, md);
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.putMetadata("", md);
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.putMetadata(" ", md);
            });
    }

    public void putDirectoryTest() throws Exception {
        Assert.assertThrows(NullPointerException.class, () -> {
                client.putDirectory(null);
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.putDirectory("");
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.putDirectory(" ");
            });
    }

    public void putSnapLink() throws Exception {
        Assert.assertThrows(NullPointerException.class, () -> {
                client.putSnapLink(null, "/foo/stor/bar", null);
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.putSnapLink("", "/foo/stor/bar", null);
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.putSnapLink(" ", "/foo/stor/bar", null);
            });
        Assert.assertThrows(NullPointerException.class, () -> {
                client.putSnapLink("/foo/stor/bar", null, null);
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.putSnapLink("/foo/stor/bar", "", null);
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.putSnapLink("/foo/stor/bar", " ", null);
            });
    }

    public void moveTest() throws Exception {
        Assert.assertThrows(NullPointerException.class, () -> {
                client.move(null, "/foo/stor/bar");
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.move("", "/foo/stor/bar");
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.move(" ", "/foo/stor/bar");
            });
        Assert.assertThrows(NullPointerException.class, () -> {
                client.move("/foo/stor/bar", null);
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.move("/foo/stor/bar", "");
            });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
                client.move("/foo/stor/bar", " ");
            });
    }
}
