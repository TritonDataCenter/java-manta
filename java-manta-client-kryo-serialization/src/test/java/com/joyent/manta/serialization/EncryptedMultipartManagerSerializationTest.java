/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.multipart.EncryptedMultipartUpload;
import com.joyent.manta.config.DefaultsConfigContext;
import org.bouncycastle.crypto.macs.HMac;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.crypto.SecretKey;
import java.util.Base64;

/**
 * Unit tests that attempt to serialize objects in the graph of a
 * {@link EncryptedMultipartUpload} class.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
@Test
public class EncryptedMultipartManagerSerializationTest {
    private SecretKey secretKey;
    private SupportedCipherDetails cipherDetails;
    private Kryo kryo = new Kryo();

    @BeforeClass
    public void setup() {
        kryo.register(HMac.class, new HmacSerializer(kryo));
        this.cipherDetails = DefaultsConfigContext.DEFAULT_CIPHER;
        byte[] keyBytes = Base64.getDecoder().decode("qAnCNUmmFjUTtImNGv241Q==");
        this.secretKey = SecretKeyUtils.loadKey(keyBytes, cipherDetails);
    }
}
