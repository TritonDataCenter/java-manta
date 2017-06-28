package com.joyent.manta.exception;

import com.joyent.manta.util.MantaVersion;
import org.apache.http.HttpRequest;
import org.apache.http.client.methods.HttpPut;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by tomascelaya on 6/28/17.
 */
public class MantaChecksumFailedExceptionTest {

    @Test
    public void containsExpectedFields() throws URISyntaxException {
        HttpRequest request = new HttpPut(new URI("http://test.manta.io/account/file"));


        MantaChecksumFailedException e = new MantaChecksumFailedException();
        String sdkVersion = e.getContextValues("mantaSdkVersion").get(0).toString();
        Assert.assertNotNull(sdkVersion,
                "SDK version data should be in exception context");
        Assert.assertEquals(sdkVersion, MantaVersion.VERSION,
                "SDK version should equal value coded");
    }



}
