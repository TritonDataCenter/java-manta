/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import com.joyent.manta.client.crypto.EncryptingEntityHelper;
import com.joyent.manta.client.crypto.EncryptingPartEntity;
import com.joyent.manta.client.crypto.EncryptionContext;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.http.entity.ExposedStringEntity;
import org.apache.commons.io.input.BrokenInputStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.http.entity.InputStreamEntity;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

@Test
public class EncryptingPartEntityTest {

    private EncryptionState state;

    private static final ByteArrayOutputStream BYTE_ARRAY_OUTPUT_STREAM_EMPTY = new ByteArrayOutputStream(0);

    private static final EncryptingPartEntity.LastPartCallback CALLBACK_NOOP =
            new EncryptingPartEntity.LastPartCallback() {
                @Override
                public ByteArrayOutputStream call(final long uploadedBytes) throws IOException {
                    return BYTE_ARRAY_OUTPUT_STREAM_EMPTY;
                }
            };

    @BeforeMethod
    public void setup() throws Exception {
        final SupportedCipherDetails cipherDetails = DefaultsConfigContext.DEFAULT_CIPHER;
        state = new EncryptionState(new EncryptionContext(SecretKeyUtils.generate(cipherDetails), cipherDetails));

        state.setMultipartStream(
                new MultipartOutputStream(
                        state.getEncryptionContext().getCipherDetails().getBlockSizeInBytes()));
        state.setCipherStream(
                EncryptingEntityHelper.makeCipherOutputForStream(
                        state.getMultipartStream(),
                        state.getEncryptionContext()));
    }

    public void doesNotCloseSuppliedOutputStreamWhenFailureOccurs() throws Exception {
        final ExposedStringEntity contentEntity = new ExposedStringEntity(
                RandomStringUtils.randomAlphanumeric(RandomUtils.nextInt(500, 1500)),
                StandardCharsets.UTF_8);

        final EncryptingPartEntity encryptingPartEntity = new EncryptingPartEntity(
                state.getCipherStream(),
                state.getMultipartStream(),
                contentEntity,
                CALLBACK_NOOP);

        final OutputStream output = Mockito.mock(OutputStream.class);
        encryptingPartEntity.writeTo(output);
        Mockito.verify(output, Mockito.never()).close();
    }

    public void doesNotCloseSuppliedOutputStreamWhenWrittenSuccessfully() throws Exception {
        final EncryptingPartEntity encryptingPartEntity = new EncryptingPartEntity(
                state.getCipherStream(),
                state.getMultipartStream(),
                new InputStreamEntity(new BrokenInputStream(new IOException("bad input"))),
                CALLBACK_NOOP);

        final OutputStream output = Mockito.mock(OutputStream.class);
        Assert.assertThrows(IOException.class, () -> encryptingPartEntity.writeTo(output));
        Mockito.verify(output, Mockito.never()).close();
    }

}
