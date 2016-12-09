/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.joyent.manta.config.EncryptionObjectAuthenticationMode;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.testng.Assert.assertEquals;

/**
 * Test class for all-purpose utility methods.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
@Test
public class MantaUtilsTest {
    public final void lastCharMatchesWithStringBuilder() {
        char match = '/';
        StringBuilder builder = new StringBuilder("/some/directory/path/");

        boolean actual = MantaUtils.endsWith(builder, match);

        Assert.assertTrue(actual, "Unable to match last character in StringBuilder");
    }

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

    public final void doesntMatchOnEmptyStringBuilder() {
        char match = '/';
        StringBuilder builder = new StringBuilder();

        boolean actual = MantaUtils.endsWith(builder, match);

        Assert.assertFalse(actual, "Matched last character in StringBuilder when we shouldn't have because it was empty");
    }

    public final void canParseAccountWithNoSubuser() {
        final String account = "username";
        final String[] parts = MantaUtils.parseAccount(account);

        Assert.assertEquals(parts.length, 1,
                "Only a single element should be present");
        Assert.assertEquals(parts[0], account,
                "First element should be username");
    }

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

    public void lastItemInPathIsCorrectFile() {
        final String expected = new UUID(24, 48).toString();
        final String path = String.format("/foo/bar/%s", expected);

        final String actual = MantaUtils.lastItemInPath(path);

        Assert.assertEquals(actual, expected);
    }

    public void lastItemInPathIsCorrectFileWithTrailingSeparator() {
        final String expected = new UUID(24, 48).toString();
        final String path = String.format("/foo/bar/%s/", expected);

        final String actual = MantaUtils.lastItemInPath(path);

        Assert.assertEquals(actual, expected);
    }

    public void canParseBooleanAsTrue() {
        String[] trues = new String[] {
                "true", "True", "TRUE", "T", "t", "TrUe", "1", "yes", "y"
        };

        for (String s : trues) {
            String errMsg = String.format("[%s] wasn't parsed as true", s);
            Assert.assertTrue(MantaUtils.parseBooleanOrNull(s), errMsg);
        }
    }

    public void canParseBooleanAsFalse() {
        String[] falses = new String[] {
                "false", "False", "FALSE", "F", "f", "FaLsE", "0", "no", "n"
        };

        for (String s : falses) {
            String errMsg = String.format("[%s] wasn't parsed as false", s);
            Assert.assertFalse(MantaUtils.parseBooleanOrNull(s), errMsg);
        }
    }

    public void canParseEnumValue() {
        EncryptionObjectAuthenticationMode expected = EncryptionObjectAuthenticationMode.Optional;
        String enumValue = expected.toString();

        EncryptionObjectAuthenticationMode actual = MantaUtils.parseEnumOrNull(
                enumValue, EncryptionObjectAuthenticationMode.class);

        Assert.assertEquals(actual, expected, "Parsed enum value doesn't match");
    }

    public void wontParseInvalidEnumValue() {
        String enumValue = "Invalid";

        EncryptionObjectAuthenticationMode actual = MantaUtils.parseEnumOrNull(
                enumValue, EncryptionObjectAuthenticationMode.class);

        Assert.assertNull(actual, "Parsed enum value should be null");
    }

    public void csvSingleCSVIsItself() {
        final String expected = "hello";
        final String csv = MantaUtils.csv(Collections.singletonList(expected));
        assertEquals(csv, expected, "Expected no delimiter");
    }

    public void csvMultipleValuesConvertToCSV() {
        final String expected = "hello, goodbye, farewell";
        final Iterable<String> list = Arrays.asList("hello", "goodbye", "farewell");
        final String csv = MantaUtils.csv(list);
        assertEquals(csv, expected, "Expected proper delimiters of comma space");
    }

    public void csvConvertsIpAddressesProperly() throws Exception {
        final String expected = "192.168.24.99, 10.10.10.22, 0.0.0.0, 2a03:2880:2110:df07:face:b00c:0:1";
        final Iterable<?> list = Arrays.asList(
                "192.168.24.99",
                InetAddress.getByName("10.10.10.22"),
                InetAddress.getByName("0.0.0.0"),
                InetAddress.getByName("2a03:2880:2110:df07:face:b00c:0:1"));
        final String csv = MantaUtils.csv(list);
        assertEquals(csv, expected, "Expected proper conversion to IP address");
    }

    public void csvChangesCommasInInputToSpaces() {
        final Iterable<String> list = Arrays.asList("hello", "goodbye,", "farewell");
        final String csv = MantaUtils.csv(list);
        assertEquals(csv, "hello, goodbye , farewell");
    }

    public void csvCanSerializeStringMap() {
        final String expected = "foo: bar, hello: goodbye";
        final Map<String, String> map = new LinkedHashMap<>();
        map.put("foo", "bar");
        map.put("hello", "goodbye");
        final String csv = MantaUtils.csv(map);

        assertEquals(csv, expected, "Map values should convert to <key>: <value>");
    }

    public void csvCanSerializeObjectMap() {
        final String expected = "foo: bar, hello: goodbye";
        final Map<Object, Object> map = new LinkedHashMap<>();
        map.put(new StringBuilder("foo"), "bar");
        map.put(new StringBuilder("hello"), "goodbye");
        final String csv = MantaUtils.csv(map);

        assertEquals(csv, expected, "Map values should convert to <key>: <value>");
    }

    public void csvWithoutDelimiterToStringCollection() {
        final Collection<String> expected = Collections.singletonList("hello");
        final Collection<String> actual = MantaUtils.fromCsv("hello");
        assertEquals(actual, expected, "expecting collection with only 'hello'");
    }

    public void csvMultipleValuesToStringCollection() {
        final Collection<String> expected = Arrays.asList("hello", "goodbye", "farewell");
        final Collection<String> actual = MantaUtils.fromCsv("hello, goodbye, farewell");
        assertEquals(actual, expected, "expecting collection to match CSV value");
    }

    public void canCreateStringMapFromObjectMap() {
        Map<String, Object> objectMap = new LinkedHashMap<>();
        objectMap.put("key1", 12);
        objectMap.put("key2", Instant.ofEpochMilli(72));
        objectMap.put("key3", Arrays.asList("hello", "goodbye"));
        objectMap.put("key4", Arrays.asList("one", "two", "three"));

        Map<String, String> stringMap = MantaUtils.asStringMap(objectMap);

        String expected = "{key1=12, key2=1970-01-01T00:00:00.072Z, key3=hello, goodbye, key4=one, two, three}";
        @SuppressWarnings("unchecked")
        String actual = StringUtils.join(stringMap);

        assertEquals(actual, expected, "We should be able to transparently "
                + "convert object maps to string maps");
    }

    public void csvWithoutDelimiterToStringArray() {
        final String[] expected = new String[] { "hello" };
        final String[] actual = MantaUtils.csv2array("hello");
        assertEquals(actual, expected, "expecting array with only 'hello'");
    }

    public void csvMultipleValuesToStringArray() {
        final String[] expected = new String[] { "hello", "goodbye", "farewell" };
        final String[] actual = MantaUtils.csv2array("hello, goodbye, farewell");
        assertEquals(actual, expected, "expecting array to match CSV value");
    }
}
