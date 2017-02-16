/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.joyent.manta.client.crypto.EncryptionContext;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.multipart.EncryptedMultipartUpload;
import com.joyent.manta.client.multipart.EncryptionState;
import com.joyent.manta.client.multipart.ServerSideMultipartUpload;
import com.joyent.manta.config.DefaultsConfigContext;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;

@Test
public class SerializationHelperTest {
    private byte[] keyBytes = Base64.getDecoder().decode("qAnCNUmmFjUTtImNGv241Q==");
    private SupportedCipherDetails cipherDetails = DefaultsConfigContext.DEFAULT_CIPHER;
    private SecretKey secretKey = SecretKeyUtils.loadKey(keyBytes, cipherDetails);
    private Kryo kryo = new Kryo();
    private SerializationHelper<ServerSideMultipartUpload> helper =
            new SerializationHelper<>(kryo, secretKey, cipherDetails, ServerSideMultipartUpload.class);

    public void canSerializeAndDeserializeUpload() throws IOException {
        final UUID uploadId = new UUID(0L, 0L);
        final String path = "/user/stor/myObject";
        final String partsDir = "/user/uploads/0/" + uploadId;
        final ServerSideMultipartUpload inner = new ServerSideMultipartUpload(uploadId, path, partsDir);
        final EncryptionContext encryptionContext = new EncryptionContext(secretKey, cipherDetails);
        final EncryptionState encryptionState = new EncryptionState(encryptionContext);
        @SuppressWarnings("unchecked")
        final EncryptedMultipartUpload<ServerSideMultipartUpload> upload =
                (EncryptedMultipartUpload<ServerSideMultipartUpload>)newUploadInstance(inner, encryptionState);

        final byte[] serializedData = helper.serialize(upload);
        final EncryptedMultipartUpload deserialized = helper.deserialize(serializedData);
        System.out.println(deserialized);
    }

    public void canConcatenateByteArrays() {
        byte[] array1 = "This is the first array. ".getBytes(StandardCharsets.US_ASCII);
        byte[] array2 = "This is the second array. ".getBytes(StandardCharsets.US_ASCII);
        byte[] array3 = "This is the third array.".getBytes(StandardCharsets.US_ASCII);

        String expected = "This is the first array. This is the second array. This is the third array.";

        byte[] concatenated = SerializationHelper.addAll(array1, array2, array3);
        String actual = new String(concatenated, StandardCharsets.US_ASCII);
        Assert.assertEquals(actual, expected);
    }

    private EncryptedMultipartUpload newUploadInstance(final Object... params) {
        try {
            return ConstructorUtils.invokeConstructor(EncryptedMultipartUpload.class,
                    params);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }
}
