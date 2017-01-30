package com.joyent.manta.http.entity;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class ExposedStringEntityTest {
    public void canSetContentLength() {
        final String string = "I am a string";
        ExposedStringEntity entity = new ExposedStringEntity(string, "US-ASCII");

        Assert.assertEquals(entity.getContentLength(),
                string.length(), "String length should equal content-length");
    }
}
