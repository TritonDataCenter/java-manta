/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;

/**
 * Unit test class that verifies the correct functioning of {@link MantaMetadata}.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaMetadataTest {
    @Test
    public void canAddMetadataKeyValue() {
        MantaMetadata instance = new MantaMetadata();
        instance.put("m-hello", "world");
        Assert.assertEquals(instance.get("m-hello"), "world");
    }

    @Test(expectedExceptions = {IllegalArgumentException.class})
    public void cantAddNullMetadataKey() {
        MantaMetadata instance = new MantaMetadata();
        instance.put(null, "world");
    }

    @Test(expectedExceptions = {IllegalArgumentException.class})
    public void cantAddEmptyMetadataKey() {
        MantaMetadata instance = new MantaMetadata();
        instance.put("", "world");
    }

    @Test(expectedExceptions = {IllegalArgumentException.class})
    public void cantAddMetadataKeyThatDoesntBeginWithM() {
        MantaMetadata instance = new MantaMetadata();
        instance.put("hello", "world");
    }

    @Test(expectedExceptions = {IllegalArgumentException.class})
    public void cantAddMetadataKeyThatContainsSpace() {
        MantaMetadata instance = new MantaMetadata();
        instance.put("m-hello my dear", "world");
    }

    @Test(expectedExceptions = {IllegalArgumentException.class})
    public void cantAddMetadataKeyThatIsntISO88591() {
        MantaMetadata instance = new MantaMetadata();
        String key = "m-\u3053\u3093\u306B\u3061\u306F";
        instance.put(key, "world");
    }

    @Test
    public void cantAddMetadataKeyThatIllegalCharacter() {
        MantaMetadata instance = new MantaMetadata();

        for (char c : MantaMetadata.ILLEGAL_KEY_CHARS) {
            boolean caught = false;
            String key = String.format("m-key-%s-suffix", c);

            try {
                instance.put(key, "world");
            } catch (final IllegalArgumentException e) {
                caught = true;
            }

            Assert.assertTrue(caught, String.format("No exception thrown for char: %s", c));
        }
    }

    // Until MANTA-3527 is resolved we should prevent users from using non-ascii values for both keys
    // and values.
    @Test
    public void cantAddMetadataKeyWithNonAsciiCharacters() {
        final MantaMetadata instance = new MantaMetadata();
        final CharsetEncoder asciiEncoder = StandardCharsets.US_ASCII.newEncoder();

        final char[] firstNonAsciiCharacter = Character.toChars(128);
        final String badChar = String.format("%s", firstNonAsciiCharacter[0]);
        Assert.assertFalse(asciiEncoder.canEncode(badChar));

        Assert.expectThrows(IllegalArgumentException.class, () -> instance.put(String.format("m-%s", badChar), "value"));
        Assert.expectThrows(IllegalArgumentException.class, () -> instance.put("m-key", badChar));
        Assert.expectThrows(IllegalArgumentException.class, () -> instance.put(String.format("m-%s", badChar), badChar));
    }

    @Test
    public void canMarkKeyAsDeleted() {
        MantaMetadata instance = new MantaMetadata();
        instance.put("m-hello", "world");
        instance.delete("m-hello");

        Assert.assertTrue(instance.containsKey("m-hello"),
                "Deleting a metadata entry means marking its value as null"
                        + "but we still need the key stored.");
        Assert.assertNull(instance.get("m-hello"));
    }

    @Test
    public void autoConvertKeysToLowercase() {
        MantaMetadata instance = new MantaMetadata();
        instance.put("M-HELLO", "world");
        Assert.assertEquals(instance.get("m-hello"), "world");
        Assert.assertEquals(instance.get("M-HELLO"), "world");
        Assert.assertEquals(instance.get("M-HELlO"), "world");
    }
}
