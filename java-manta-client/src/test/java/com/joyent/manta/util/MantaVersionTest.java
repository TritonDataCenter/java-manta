package com.joyent.manta.util;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class MantaVersionTest {
    public void canLoadVersion() {
        Assert.assertNotNull(MantaVersion.VERSION);
    }

    public void canLoadVersionDate() {
        Assert.assertNotNull(MantaVersion.DATE);
    }
}
