package com.joyent.manta.client.multipart;

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.stream.Stream;

@Test
public class TestMultipartManagerTest {
    MantaMultipartManager manager = new TestMultipartManager();

    public void canDoMultipartUpload() throws IOException {
        MantaMultipartUpload upload = manager.initiateUpload("/user/stor/testobject");

        MantaMultipartUploadPart[] parts = new MantaMultipartUploadPart[]{
                manager.uploadPart(upload, 1, "Line 1\n"),
                manager.uploadPart(upload, 2, "Line 2\n"),
                manager.uploadPart(upload, 3, "Line 3\n"),
                manager.uploadPart(upload, 4, "Line 4\n"),
                manager.uploadPart(upload, 5, "Line 5")
        };

        Stream<MantaMultipartUploadTuple> partsStream = Stream.of(parts);

        manager.complete(upload, partsStream);

        TestMultipartUpload actualUpload = (TestMultipartUpload)upload;
        String actual = FileUtils.readFileToString(actualUpload.getContents(), "UTF-8");

        String expected = "Line 1\nLine 2\nLine 3\nLine 4\nLine 5";

        Assert.assertEquals(actual, expected);
    }
}
