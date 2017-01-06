package com.joyent.manta.client.crypto;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class BouncyCastleLoaderTest {
    public void canLoadBouncyCastleProvider() {
        Assert.assertNotNull(BouncyCastleLoader.BOUNCY_CASTLE_PROVIDER,
                "Bouncy Castle provider wasn't loaded");
    }
}
