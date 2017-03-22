/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.joyent.manta.client.crypto.EncryptingEntityHelper;
import com.joyent.manta.client.crypto.EncryptionContext;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.multipart.EncryptedMultipartUpload;
import com.joyent.manta.client.multipart.EncryptionState;
import com.joyent.manta.client.multipart.MultipartOutputStream;
import com.joyent.manta.client.multipart.ServerSideMultipartUpload;
import com.joyent.manta.config.DefaultsConfigContext;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.crypto.SecretKey;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.Base64;
import java.util.UUID;

/**
 * Unit tests that attempt to serialize objects in the graph of a
 * {@link EncryptedMultipartUpload} class.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
@Test
public class EncryptedMultipartManagerSerializationTest {
    private byte[] keyBytes = Base64.getDecoder().decode("qAnCNUmmFjUTtImNGv241Q==");
    private SupportedCipherDetails cipherDetails = DefaultsConfigContext.DEFAULT_CIPHER;
    private SecretKey secretKey = SecretKeyUtils.loadKey(keyBytes, cipherDetails);
    private Kryo kryo = new Kryo();

    @BeforeClass
    public void setup() {
        kryo.register(EncryptedMultipartUpload.class,
                new EncryptedMultipartSerializer<>(kryo, EncryptedMultipartUpload.class,
                        ServerSideMultipartUpload.class, secretKey));
    }

    public void canSerializeEncryptedServerSideMultipartUpload() throws IOException {
        final UUID uploadId = new UUID(0L, 0L);
        final String path = "/user/stor/myObject";
        final String partsDir = "/user/uploads/0/" + uploadId;
        final ServerSideMultipartUpload inner = new ServerSideMultipartUpload(uploadId, path, partsDir);
        final EncryptionContext encryptionContext = new EncryptionContext(secretKey, cipherDetails);
        final EncryptionState encryptionState = new EncryptionState(encryptionContext);

        Field cipherStreamField = ReflectionUtils.getField(EncryptionState.class, "cipherStream");
        MultipartOutputStream multipartStream = new MultipartOutputStream(cipherDetails.getBlockSizeInBytes());
        OutputStream cipherStream = EncryptingEntityHelper.makeCipherOutputForStream(
                multipartStream, encryptionContext);

        try {
            FieldUtils.writeField(cipherStreamField, encryptionState, cipherStream);
        } catch (IllegalAccessException e) {
            throw new AssertionError(e);
        }

        final EncryptedMultipartUpload<?> upload = newUploadInstance(inner, encryptionState);

        final byte[] serializedData;

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
             Output output = new Output(outputStream)) {
            this.kryo.writeObject(output, upload);
            output.flush();
            serializedData = outputStream.toByteArray();
        }

        try (Input input = new Input(serializedData)) {
            final EncryptedMultipartUpload<?> actual = kryo.readObject(
                    input, EncryptedMultipartUpload.class);
            Assert.assertEquals(actual, upload);
        }
    }

    private EncryptedMultipartUpload<?> newUploadInstance(final Object... params) {
        try {
            return ConstructorUtils.invokeConstructor(EncryptedMultipartUpload.class,
                    params);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }
}
