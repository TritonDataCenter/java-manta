/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import com.joyent.manta.client.MantaMetadata;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;

@Test
public class EncryptedMetadataUtilsTest {
    public void canConvertPlaintextToMapWithTypicalValues() {
        StringBuilder builder = new StringBuilder();
        builder.append("header1: value1\n");
        builder.append("Header2: Value2");

        Map<String, String> map = EncryptedMetadataUtils.plaintextMetadataAsMap(
                builder.toString());

        Assert.assertEquals(map.get("header1"), "value1");
        Assert.assertEquals(map.get("Header2"), "Value2");
        Assert.assertEquals(2, map.size());
    }

    public void canConvertPlaintextToMapWithEmbeddedColon() {
        StringBuilder builder = new StringBuilder();
        builder.append("header3: Value3: with a colon");

        Map<String, String> map = EncryptedMetadataUtils.plaintextMetadataAsMap(
                builder.toString());

        Assert.assertEquals(map.get("Header3"), "Value3: with a colon");
        Assert.assertEquals(1, map.size());
    }

    public void canConvertPlaintextToMapWithMixedUpSpaces() {
        StringBuilder builder = new StringBuilder();
        builder.append("header4:value4\n");
        builder.append("header5: value5\n");
        builder.append("header6:       value6\n");

        Map<String, String> map = EncryptedMetadataUtils.plaintextMetadataAsMap(
                builder.toString());

        Assert.assertEquals(map.get("header4"), "value4");
        Assert.assertEquals(map.get("header5"), "value5");
        Assert.assertEquals(map.get("header6"), "value6");

        Assert.assertEquals(3, map.size());
    }

    public void canConvertPlaintextToMapWithMixedLFs() {
        StringBuilder builder = new StringBuilder();
        builder.append("\n");
        builder.append("header7: value7\n");
        builder.append("\n\n\n");
        builder.append("header8: value8\n\n\n");

        Map<String, String> map = EncryptedMetadataUtils.plaintextMetadataAsMap(
                builder.toString());

        Assert.assertEquals(map.get("header7"), "value7");
        Assert.assertEquals(map.get("header8"), "value8");

        Assert.assertEquals(2, map.size());
    }

    public void canConvertEncryptedMetadataToString() throws IOException {
        MantaMetadata metadata = new MantaMetadata();
        metadata.put("m-some-key", "unencrypted metadata");
        metadata.put("e-key-1", "value 1");
        metadata.put("e-KEY-2", "value 2");
        metadata.put("E-key-3", "value 3");

        String actual = EncryptedMetadataUtils.encryptedMetadataAsString(metadata);

        Map<String, String> metadataMap = EncryptedMetadataUtils.plaintextMetadataAsMap(actual);

        for (Map.Entry<String, String> entry : metadataMap.entrySet()) {
            Assert.assertNotNull(entry.getKey());
            Assert.assertNotNull(entry.getValue());

            Assert.assertEquals(entry.getValue(),
                    metadata.get(entry.getKey()),
                    "Couldn't find element in metadata source");
        }

        long expectedElements = metadata.keySet()
                .stream()
                .filter(k -> k.startsWith(MantaMetadata.ENCRYPTED_METADATA_PREFIX))
                .count();

        Assert.assertEquals(metadataMap.size(), expectedElements,
                "Mismatch between metadata size and actual headers returned");
    }
}
