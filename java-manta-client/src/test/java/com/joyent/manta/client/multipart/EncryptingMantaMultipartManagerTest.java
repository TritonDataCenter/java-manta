package com.joyent.manta.client.multipart;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.crypto.AesCtrCipherDetails;
import com.joyent.manta.client.crypto.EncryptionContext;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.exception.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.crypto.SecretKey;

import java.io.IOException;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Test
public class EncryptingMantaMultipartManagerTest {
    TestMultipartManager testManager = new TestMultipartManager();
    EncryptingMantaMultipartManager<TestMultipartManager, TestMultipartUpload> manager;

    @BeforeClass
    public void setup() {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
        SecretKey secretKey = SecretKeyUtils.generate(cipherDetails);

        EncryptionContext encryptionContext = new EncryptionContext(secretKey, cipherDetails);

        ConfigContext context = mock(ConfigContext.class);
        when(context.isClientEncryptionEnabled()).thenReturn(true);
        MantaClient mantaClient = mock(MantaClient.class);
        when(mantaClient.getContext()).thenReturn(context);

        manager = new EncryptingMantaMultipartManager<>(encryptionContext, mantaClient, testManager);
    }

    public void canDoMultipartUpload() throws IOException {
        EncryptedMultipartUpload<TestMultipartUpload> upload = manager.initiateUpload("/user/stor/testobject");

        MantaMultipartUploadPart[] parts = new MantaMultipartUploadPart[]{
                manager.uploadPart(upload, 1, "Line 1\n"),
                manager.uploadPart(upload, 2, "Line 2\n"),
                manager.uploadPart(upload, 3, "Line 3\n"),
                manager.uploadPart(upload, 4, "Line 4\n"),
                manager.uploadPart(upload, 5, "Line 5")
        };

        Stream<MantaMultipartUploadTuple> partsStream = Stream.of(parts);

        manager.complete(upload, partsStream);

        TestMultipartUpload actualUpload = upload.getWrapped();
        String actual = FileUtils.readFileToString(actualUpload.getContents(), "UTF-8");

        String expected = "Line 1\nLine 2\nLine 3\nLine 4\nLine 5";

        Assert.assertEquals(actual, expected);
    }
}
