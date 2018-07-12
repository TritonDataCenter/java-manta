/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.exception.MantaClientEncryptionCiphertextAuthenticationException;
import com.joyent.manta.exception.MantaIOException;
import com.joyent.manta.http.InputStreamContinuator;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.http.entity.MantaInputStreamEntity;
import com.joyent.manta.util.AutoContinuingInputStream;
import com.joyent.manta.util.FailingInputStream;
import com.joyent.manta.util.FileInputStreamContinuator;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.conn.EofSensorInputStream;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

@Test
public class MantaEncryptedObjectInputStreamTest {

    private final Path testFile;
    private final URL testURL;
    private final int plaintextSize;
    private final byte[] plaintextBytes;

    public MantaEncryptedObjectInputStreamTest() {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        this.testURL = classLoader.getResource("test-data/chaucer.txt");

        try {
            testFile = Paths.get(testURL.toURI());
        } catch (URISyntaxException e) {
            throw new AssertionError(e);
        }

        this.plaintextSize = (int)testFile.toFile().length();

        try (InputStream in = this.testURL.openStream()) {
            this.plaintextBytes = IOUtils.readFully(in, plaintextSize);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void canDecryptEntireObjectAuthenticatedAesCbc128() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_128_BIT, true, null, null);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectAuthenticatedAesCbc192() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_192_BIT, true, null, null);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectAuthenticatedAesCbc256() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_256_BIT, true, null, null);
    }

    public void canDecryptEntireObjectAuthenticatedAesCtr128() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_128_BIT, true, null, null);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectAuthenticatedAesCtr192() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_192_BIT, true, null, null);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectAuthenticatedAesCtr256() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_256_BIT, true, null, null);
    }

    public void canDecryptEntireObjectAuthenticatedAesGcm128() throws IOException {
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_128_BIT, true, null, null);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectAuthenticatedAesGcm192() throws IOException {
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_192_BIT, true, null, null);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectAuthenticatedAesGcm256() throws IOException {
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_256_BIT, true, null, null);
    }


    public void canDecryptEntireObjectAuthenticatedWithFailureAutoRecoveryAesCbc128() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailure(AesCbcCipherDetails.INSTANCE_128_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectAuthenticatedWithFailureAutoRecoveryAesCbc192() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailure(AesCbcCipherDetails.INSTANCE_192_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectAuthenticatedWithFailureAutoRecoveryAesCbc256() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailure(AesCbcCipherDetails.INSTANCE_256_BIT, true);
    }

    public void canDecryptEntireObjectAuthenticatedWithFailureAutoRecoveryAesCtr128() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailure(AesCtrCipherDetails.INSTANCE_128_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectAuthenticatedWithFailureAutoRecoveryAesCtr192() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailure(AesCtrCipherDetails.INSTANCE_192_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectAuthenticatedWithFailureAutoRecoveryAesCtr256() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailure(AesCtrCipherDetails.INSTANCE_256_BIT, true);
    }

    public void canDecryptEntireObjectAuthenticatedWithFailureAutoRecoveryAesGcm128() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailure(AesGcmCipherDetails.INSTANCE_128_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectAuthenticatedWithFailureAutoRecoveryAesGcm192() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailure(AesGcmCipherDetails.INSTANCE_192_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectAuthenticatedWithFailureAutoRecoveryAesGcm256() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailure(AesGcmCipherDetails.INSTANCE_256_BIT, true);
    }

    private void canDecryptEntireObjectAllReadModesWithFailure(final SupportedCipherDetails cipherDetails,
                                                               final boolean authenticate)
            throws IOException {
        System.out.printf("Testing decryption of [%s] as full read of stream%n",
                          cipherDetails.getCipherId());

        // canDecryptEntireObject(cipherDetails, SingleReads.class, authenticate, 0f, false);
        // canDecryptEntireObject(cipherDetails, SingleReads.class, authenticate, 0f, true);
        // canDecryptEntireObject(cipherDetails, ByteChunkReads.class, authenticate, 0f, false);
        // canDecryptEntireObject(cipherDetails, ByteChunkReads.class, authenticate, 0f, true);
        // canDecryptEntireObject(cipherDetails, ByteChunkOffsetReads.class, authenticate, 0f, false);
        // canDecryptEntireObject(cipherDetails, ByteChunkOffsetReads.class, authenticate, 0f, true);
        //
        // canDecryptEntireObject(cipherDetails, SingleReads.class, authenticate, 0.5f, false);
        // canDecryptEntireObject(cipherDetails, SingleReads.class, authenticate, 0.5f, true);
        // canDecryptEntireObject(cipherDetails, ByteChunkReads.class, authenticate, 0.5f, false);
        // canDecryptEntireObject(cipherDetails, ByteChunkReads.class, authenticate, 0.5f, true);
        // canDecryptEntireObject(cipherDetails, ByteChunkOffsetReads.class, authenticate, 0.5f, false);
        // canDecryptEntireObject(cipherDetails, ByteChunkOffsetReads.class, authenticate, 0.5f, true);
    }

    public void canDecryptEntireObjectUnauthenticatedAesCbc128() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_128_BIT, false, null, null);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectUnauthenticatedAesCbc192() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_192_BIT, false, null, null);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectUnauthenticatedAesCbc256() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_256_BIT, false, null, null);
    }

    public void canDecryptEntireObjectUnauthenticatedAesCtr128() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_128_BIT, false, null, null);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectUnauthenticatedAesCtr192() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_192_BIT, false, null, null);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectUnauthenticatedAesCtr256() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_256_BIT, false, null, null);
    }

    public void canDecryptEntireObjectUnauthenticatedAesGcm128() throws IOException {
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_128_BIT, false, null, null);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectUnauthenticatedAesGcm192() throws IOException {
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_192_BIT, false, null, null);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectUnauthenticatedAesGcm256() throws IOException {
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_256_BIT, false, null, null);
    }


    public void canDecryptEntireObjectUnauthenticatedWithFailureAutoRecoveryAesCbc128() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_128_BIT, false, 0f, true);
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_128_BIT, false, 0f, false);

        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_128_BIT, false, 0.5f, true);
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_128_BIT, false, 0.5f, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectUnauthenticatedWithFailureAutoRecoveryAesCbc192() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_192_BIT, false, 0f, true);
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_192_BIT, false, 0f, false);

        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_192_BIT, false, 0.5f, true);
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_192_BIT, false, 0.5f, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectUnauthenticatedWithFailureAutoRecoveryAesCbc256() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_256_BIT, false, 0f, true);
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_256_BIT, false, 0f, false);

        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_256_BIT, false, 0.5f, true);
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_256_BIT, false, 0.5f, true);
    }

    public void canDecryptEntireObjectUnauthenticatedWithFailureAutoRecoveryAesCtr128() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_128_BIT, false, 0f, true);
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_128_BIT, false, 0f, false);

        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_128_BIT, false, 0.5f, true);
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_128_BIT, false, 0.5f, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectUnauthenticatedWithFailureAutoRecoveryAesCtr192() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_192_BIT, false, 0f, true);
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_192_BIT, false, 0f, false);

        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_192_BIT, false, 0.5f, true);
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_192_BIT, false, 0.5f, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectUnauthenticatedWithFailureAutoRecoveryAesCtr256() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_256_BIT, false, 0f, true);
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_256_BIT, false, 0f, false);

        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_256_BIT, false, 0.5f, true);
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_256_BIT, false, 0.5f, true);
    }

    public void canDecryptEntireObjectUnauthenticatedWithFailureAutoRecoveryAesGcm128() throws IOException {
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_128_BIT, false, 0f, true);
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_128_BIT, false, 0f, false);

        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_128_BIT, false, 0.5f, true);
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_128_BIT, false, 0.5f, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectUnauthenticatedWithFailureAutoRecoveryAesGcm192() throws IOException {
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_192_BIT, false, 0f, true);
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_192_BIT, false, 0f, false);

        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_192_BIT, false, 0.5f, true);
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_192_BIT, false, 0.5f, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectUnauthenticatedWithFailureAutoRecoveryAesGcm256() throws IOException {
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_256_BIT, false, 0f, true);
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_256_BIT, false, 0f, false);

        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_256_BIT, false, 0.5f, true);
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_256_BIT, false, 0.5f, true);
    }


    public void willErrorIfCiphertextIsModifiedAesCbc128() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesCbcCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAesCbc192() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesCbcCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAesCbc256() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesCbcCipherDetails.INSTANCE_256_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAesCtr128() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAesCtr192() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesCtrCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAesCtr256() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesCtrCipherDetails.INSTANCE_256_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAesGcm128() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesGcmCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAesGcm192() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesGcmCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAesGcm256() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesGcmCipherDetails.INSTANCE_256_BIT);
    }


    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesCbc128() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesCbcCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesCbc192() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesCbcCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesCbc256() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesCbcCipherDetails.INSTANCE_256_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesCtr128() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesCtr192() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesCtrCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesCtr256() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesCtrCipherDetails.INSTANCE_256_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesGcm128() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesGcmCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesGcm192() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesGcmCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesGcm256() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesGcmCipherDetails.INSTANCE_256_BIT);
    }


    public void canSkipBytesAuthenticatedAesCbc128() throws IOException {
        canSkipBytesAuthenticated(AesCbcCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesAuthenticatedAesCbc192() throws IOException {
        canSkipBytesAuthenticated(AesCbcCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesAuthenticatedAesCbc256() throws IOException {
        canSkipBytesAuthenticated(AesCbcCipherDetails.INSTANCE_256_BIT);
    }

    public void canSkipBytesAuthenticatedAesCtr128() throws IOException {
        canSkipBytesAuthenticated(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesAuthenticatedAesCtr192() throws IOException {
        canSkipBytesAuthenticated(AesCtrCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesAuthenticatedAesCtr256() throws IOException {
        canSkipBytesAuthenticated(AesCtrCipherDetails.INSTANCE_256_BIT);
    }

    public void canSkipBytesAuthenticatedAesGcm128() throws IOException {
        canSkipBytesAuthenticated(AesGcmCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesAuthenticatedAesGcm192() throws IOException {
        canSkipBytesAuthenticated(AesGcmCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesAuthenticatedAesGcm256() throws IOException {
        canSkipBytesAuthenticated(AesGcmCipherDetails.INSTANCE_256_BIT);
    }


    public void canSkipBytesUnauthenticatedAesCbc128() throws IOException {
        canSkipBytesUnauthenticated(AesCbcCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesUnauthenticatedAesCbc192() throws IOException {
        canSkipBytesUnauthenticated(AesCbcCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesUnauthenticatedAesCbc256() throws IOException {
        canSkipBytesUnauthenticated(AesCbcCipherDetails.INSTANCE_256_BIT);
    }

    public void canSkipBytesUnauthenticatedAesCtr128() throws IOException {
        canSkipBytesUnauthenticated(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesUnauthenticatedAesCtr192() throws IOException {
        canSkipBytesUnauthenticated(AesCtrCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesUnauthenticatedAesCtr256() throws IOException {
        canSkipBytesUnauthenticated(AesCtrCipherDetails.INSTANCE_256_BIT);
    }

    public void canSkipBytesUnauthenticatedAesGcm128() throws IOException {
        canSkipBytesUnauthenticated(AesGcmCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesUnauthenticatedAesGcm192() throws IOException {
        canSkipBytesUnauthenticated(AesGcmCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesUnauthenticatedAesGcm256() throws IOException {
        canSkipBytesUnauthenticated(AesGcmCipherDetails.INSTANCE_256_BIT);
    }


    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesCbc128() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesCbcCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesCbc192() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesCbcCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesCbc256() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesCbcCipherDetails.INSTANCE_256_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesCtr128() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesCtr192() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesCtrCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesCtr256() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesCtrCipherDetails.INSTANCE_256_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesGcm128() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesGcmCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesGcm192() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesGcmCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesGcm256() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesGcmCipherDetails.INSTANCE_256_BIT);
    }

    public void canReadByteRangeStartingAtZeroEndingInFirstBlockAesCtr128() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() / 2;
        canReadByteRangeAllReadModes(cipherDetails, 0, endPos);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canReadByteRangeStartingAtZeroEndingInFirstBlockAesCtr192() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_192_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() / 2;
        canReadByteRangeAllReadModes(cipherDetails, 0, endPos);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canReadByteRangeStartingAtZeroEndingInFirstBlockAesCtr256() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_256_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() / 2;
        canReadByteRangeAllReadModes(cipherDetails, 0, endPos);
    }

    public void canReadByteRangeStartingAtZeroEndingInThirdBlockAesCtr128() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() * 2 + (cipherDetails.getBlockSizeInBytes() / 2);
        canReadByteRangeAllReadModes(cipherDetails, 0, endPos);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canReadByteRangeStartingAtZeroEndingInThirdBlockAesCtr192() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_192_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() * 2 + (cipherDetails.getBlockSizeInBytes() / 2);
        canReadByteRangeAllReadModes(cipherDetails, 0, endPos);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canReadByteRangeStartingAtZeroEndingInThirdBlockAesCtr256() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_256_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() * 2 + (cipherDetails.getBlockSizeInBytes() / 2);
        canReadByteRangeAllReadModes(cipherDetails, 0, endPos);
    }

    public void canReadByteRangeStartingAtThreeEndingInFirstBlockAesCtr128() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() / 2;
        canReadByteRangeAllReadModes(cipherDetails, 3, endPos);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canReadByteRangeStartingAtThreeEndingInFirstBlockAesCtr192() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_192_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() / 2;
        canReadByteRangeAllReadModes(cipherDetails, 3, endPos);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canReadByteRangeStartingAtThreeEndingInFirstBlockAesCtr256() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_256_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() / 2;
        canReadByteRangeAllReadModes(cipherDetails, 3, endPos);
    }

    public void canReadByteRangeStartingAtThirdBlockEndingInFifthBlockAesCtr128() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
        int startPos = cipherDetails.getBlockSizeInBytes() * 3 + (cipherDetails.getBlockSizeInBytes() / 2);
        int endPos = cipherDetails.getBlockSizeInBytes() * 5 + (cipherDetails.getBlockSizeInBytes() / 2);
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canReadByteRangeStartingAtThirdBlockEndingInFifthBlockAesCtr192() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_192_BIT;
        int startPos = cipherDetails.getBlockSizeInBytes() * 3 + (cipherDetails.getBlockSizeInBytes() / 2);
        int endPos = cipherDetails.getBlockSizeInBytes() * 5 + (cipherDetails.getBlockSizeInBytes() / 2);
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canReadByteRangeStartingAtThirdBlockEndingInFifthBlockAesCtr256() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_256_BIT;
        int startPos = cipherDetails.getBlockSizeInBytes() * 3 + (cipherDetails.getBlockSizeInBytes() / 2);
        int endPos = cipherDetails.getBlockSizeInBytes() * 5 + (cipherDetails.getBlockSizeInBytes() / 2);
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
    }

    public void canReadByteRangeStarting25bytesFromEndToEndAesCtr128() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
        int endPos = plaintextSize - 1;
        int startPos = endPos - 25;
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canReadByteRangeStarting25bytesFromEndToEndAesCtr192() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_192_BIT;
        int endPos = plaintextSize - 1;
        int startPos = endPos - 25;
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canReadByteRangeStarting25bytesFromEndToEndAesCtr256() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_256_BIT;
        int endPos = plaintextSize - 1;
        int startPos = endPos - 25;
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
    }

    public void canReadByteRangeStarting25bytesFromEndToBeyondEndAesCtr128() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
        int endPos = plaintextSize * 2;
        int startPos = plaintextSize - 26;
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canReadByteRangeStarting25bytesFromEndToBeyondEndAesCtr192() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_192_BIT;
        int endPos = plaintextSize * 2;
        int startPos = plaintextSize - 26;
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canReadByteRangeStarting25bytesFromEndToBeyondEndAesCtr256() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_256_BIT;
        int endPos = plaintextSize * 2;
        int startPos = plaintextSize - 26;
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
    }


    public void canCopyStreamWithLargeBufferBufferAuthenticatedAesCbc128() throws IOException {
         canCopyToOutputStreamWithLargeBuffer(AesCbcCipherDetails.INSTANCE_128_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canCopyStreamWithLargeBufferBufferAuthenticatedAesCbc192() throws IOException {
         canCopyToOutputStreamWithLargeBuffer(AesCbcCipherDetails.INSTANCE_192_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canCopyStreamWithLargeBufferBufferAuthenticatedAesCbc256() throws IOException {
         canCopyToOutputStreamWithLargeBuffer(AesCbcCipherDetails.INSTANCE_256_BIT, true);
    }

    public void canCopyStreamWithLargeBufferBufferAuthenticatedAesCtr128() throws IOException {
         canCopyToOutputStreamWithLargeBuffer(AesCtrCipherDetails.INSTANCE_128_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canCopyStreamWithLargeBufferBufferAuthenticatedAesCtr192() throws IOException {
         canCopyToOutputStreamWithLargeBuffer(AesCtrCipherDetails.INSTANCE_192_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canCopyStreamWithLargeBufferBufferAuthenticatedAesCtr256() throws IOException {
         canCopyToOutputStreamWithLargeBuffer(AesCtrCipherDetails.INSTANCE_256_BIT, true);
    }

    public void canCopyStreamWithLargeBufferBufferAuthenticatedAesGcm128() throws IOException {
         canCopyToOutputStreamWithLargeBuffer(AesGcmCipherDetails.INSTANCE_128_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canCopyStreamWithLargeBufferBufferAuthenticatedAesGcm192() throws IOException {
         canCopyToOutputStreamWithLargeBuffer(AesGcmCipherDetails.INSTANCE_192_BIT, true);
    }


    public void canCopyStreamWithLargeBufferBufferUnauthenticatedAesCbc128() throws IOException {
        canCopyToOutputStreamWithLargeBuffer(AesCbcCipherDetails.INSTANCE_128_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canCopyStreamWithLargeBufferBufferUnauthenticatedAesCbc192() throws IOException {
        canCopyToOutputStreamWithLargeBuffer(AesCbcCipherDetails.INSTANCE_192_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canCopyStreamWithLargeBufferBufferUnauthenticatedAesCbc256() throws IOException {
        canCopyToOutputStreamWithLargeBuffer(AesCbcCipherDetails.INSTANCE_256_BIT, false);
    }

    public void canCopyStreamWithLargeBufferBufferUnauthenticatedAesCtr128() throws IOException {
        canCopyToOutputStreamWithLargeBuffer(AesCtrCipherDetails.INSTANCE_128_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canCopyStreamWithLargeBufferBufferUnauthenticatedAesCtr192() throws IOException {
        canCopyToOutputStreamWithLargeBuffer(AesCtrCipherDetails.INSTANCE_192_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canCopyStreamWithLargeBufferBufferUnauthenticatedAesCtr256() throws IOException {
        canCopyToOutputStreamWithLargeBuffer(AesCtrCipherDetails.INSTANCE_256_BIT, false);
    }



    public void willErrorIfCiphertextIsMissingHmacAesCbc128() throws IOException {
        willErrorWhenMissingHMAC(AesCbcCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsMissingHmacAesCbc192() throws IOException {
        willErrorWhenMissingHMAC(AesCbcCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsMissingHmacAesCbc256() throws IOException {
        willErrorWhenMissingHMAC(AesCbcCipherDetails.INSTANCE_256_BIT);
    }

    public void willErrorIfCiphertextIsMissingHmacAesCtr128() throws IOException {
        willErrorWhenMissingHMAC(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsMissingHmacAesCtr192() throws IOException {
        willErrorWhenMissingHMAC(AesCtrCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsMissingHmacAesCtr256() throws IOException {
        willErrorWhenMissingHMAC(AesCtrCipherDetails.INSTANCE_256_BIT);
    }


    public void willValidateIfHmacIsReadInMultipleReadsAesCtr128() throws IOException {
        willValidateIfHmacIsReadInMultipleReads(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willValidateIfHmacIsReadInMultipleReadsAesCtr192() throws IOException {
        willValidateIfHmacIsReadInMultipleReads(AesCtrCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willValidateIfHmacIsReadInMultipleReadsAesCtr256() throws IOException {
        willValidateIfHmacIsReadInMultipleReads(AesCtrCipherDetails.INSTANCE_256_BIT);
    }

    /*

    removing failing tests documented in https://github.com/joyent/java-manta/issues/257

    public void willValidateIfHmacIsReadInMultipleReadsAesCbc128() throws IOException {
        willValidateIfHmacIsReadInMultipleReads(AesCbcCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willValidateIfHmacIsReadInMultipleReadsAesCbc192() throws IOException {
        willValidateIfHmacIsReadInMultipleReads(AesCbcCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willValidateIfHmacIsReadInMultipleReadsAesCbc256() throws IOException {
        willValidateIfHmacIsReadInMultipleReads(AesCbcCipherDetails.INSTANCE_256_BIT);
    }

    */

    /* TEST UTILITY CLASSES */

    private static class EncryptedFile {
        public final Cipher cipher;
        public final File file;

        public EncryptedFile(Cipher cipher, File file) {
            this.cipher = cipher;
            this.file = file;
        }
    }


    private static final class ReadBytesFactory {

        public static <T extends ReadBytes> ReadBytes fullStrategy(final Class<T> klass) {

            if (SingleReads.class.equals(klass)) {
                return new SingleReads();
            }


            if (ByteChunkReads.class.equals(klass)) {
                return new ByteChunkReads();
            }

            if (ByteChunkOffsetReads.class.equals(klass)) {
                return new ByteChunkOffsetReads();
            }

            throw new ClassCastException("Don't know how to build class: " + klass.getCanonicalName());
        }

        public static <T extends ReadPartialBytes> ReadBytes partialStrategy(final Class<? extends ReadPartialBytes> klass,
                                                                             final long size) {

            if (SingleBytePartialRead.class.equals(klass)) {
                return new SingleBytePartialRead();
            }


            if (ReadAndSkipPartialRead.class.equals(klass)) {
                return new ReadAndSkipPartialRead(size);
            }

            if (ReadAndSkipPartialReadStaticSkipSize.class.equals(klass)) {
                return new ReadAndSkipPartialReadStaticSkipSize(Math.toIntExact(size));
            }

            throw new ClassCastException("Don't know how to build class: " + klass.getCanonicalName());
        }

        public static <T extends ReadBytes> ReadBytes build(final Class<T> klass, final long inputSize) {
            if (ReadPartialBytes.class.isAssignableFrom(klass)) {
                return partialStrategy((Class<? extends ReadPartialBytes>) klass, inputSize);
            }

            if (ReadBytes.class.isAssignableFrom(klass)) {
                return fullStrategy(klass);
            }

            throw new ClassCastException("Don't know how to build class: " + klass.getCanonicalName());
        }

    }

    private interface ReadBytes {
        int readBytes(InputStream in, byte[] target) throws IOException;
    }

    private interface ReadPartialBytes extends ReadBytes {
    }

    private static class SingleReads implements ReadBytes {
        @Override
        public int readBytes(InputStream in, byte[] target)  throws IOException {
            int totalRead = 0;
            int lastByte;

            try {
                while ((lastByte = in.read()) != -1) {
                    target[totalRead++] = (byte) lastByte;
                }
            } catch (ArrayIndexOutOfBoundsException e) {
                while (in.read() != -1) {
                    totalRead++;
                }

                String msg = String.format("%d bytes available in stream, but "
                        + "the byte array target has a length of %d bytes",
                        totalRead, target.length);
                throw new AssertionError(msg, e);
            }

            return totalRead;
        }
    }

    private static class ByteChunkReads implements ReadBytes {
        @Override
        public int readBytes(InputStream in, byte[] target) throws IOException {
            int totalRead = 0;
            int lastRead;
            int chunkSize = 16;
            byte[] chunk = new byte[chunkSize];

            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                while ((lastRead = in.read(chunk)) != -1) {
                    totalRead += lastRead;
                    out.write(chunk, 0, lastRead);
                }

                byte[] written = out.toByteArray();
                System.arraycopy(written, 0, target, 0, target.length);
            }

            return totalRead;
        }
    }

    private static class ByteChunkOffsetReads implements ReadBytes {
        @Override
        public int readBytes(InputStream in, byte[] target) throws IOException {
            int totalRead = 0;
            int lastRead;
            int chunkSize = 128;
            byte[] chunk = new byte[512];

            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                while ((lastRead = in.read(chunk, 0, chunkSize)) != -1) {
                    totalRead += lastRead;
                    out.write(chunk, 0, lastRead);
                }

                byte[] written = out.toByteArray();
                System.arraycopy(written, 0, target, 0, target.length);
            }

            return totalRead;
        }
    }

    private static class SingleBytePartialRead implements ReadPartialBytes {
        @Override
        public int readBytes(InputStream in, byte[] target) throws IOException {
            int read = in.read();

            if (read == -1) {
                return -1;
            }

            target[0] = (byte)read;

            return 1;
        }
    }

    /**
     * This class needs to know the amount of data being read so that it can calculate skip lengths to cover at least
     * half of the stream.
     */
    private static class ReadAndSkipPartialRead implements ReadPartialBytes {

        private final long inputSize;

        ReadAndSkipPartialRead(final long inputSize) {
            this.inputSize = inputSize;
        }

        @Override
        public int readBytes(InputStream in, byte[] target) throws IOException {
            int totalRead = 0;
            int lastRead;

            if ((lastRead = in.read()) == -1) {
                return -1;
            } else {
                target[totalRead++] = (byte)lastRead;
            }

            final int skipSize = Math.toIntExact(Math.floorDiv(this.inputSize, 4));
            totalRead += Math.toIntExact(in.skip(skipSize));

            if ((lastRead = in.read()) == -1) {
                return -1;
            } else {
                target[totalRead++] = (byte)lastRead;
            }

            totalRead += Math.toIntExact(in.skip(skipSize));

            return totalRead;
        }
    }

    /**
     * Like ReadAndSkipPartialRead but allows for setting the skip size directly to test
     * skips smaller and larger than {@link MantaEncryptedObjectInputStream#DEFAULT_BUFFER_SIZE}.
     */
    private static class ReadAndSkipPartialReadStaticSkipSize implements ReadPartialBytes {

        private final int skipSize;

        ReadAndSkipPartialReadStaticSkipSize(final int skipSize) {
            this.skipSize = skipSize;
        }

        @Override
        public int readBytes(InputStream in, byte[] target) throws IOException {
            int totalRead = 0;
            int lastRead;

            if ((lastRead = in.read()) == -1) {
                return -1;
            } else {
                target[totalRead++] = (byte)lastRead;
            }

            totalRead += Math.toIntExact(in.skip(skipSize));

            if ((lastRead = in.read()) == -1) {
                return -1;
            } else {
                target[totalRead++] = (byte)lastRead;
            }

            totalRead += Math.toIntExact(in.skip(skipSize));

            return totalRead;
        }
    }

    /* TEST UTILITY METHODS */

    /**
     * Test that loops through all of the ciphers and attempts to decrypt an encrypted stream. An assertion fails if the
     * original plaintext and the decrypted plaintext don't match. Additionally, this test tries to read data from the
     * stream using three different read methods and makes sure that all read methods function correctly.
     */
    private void canDecryptEntireObjectAllReadModes(SupportedCipherDetails cipherDetails,
                                                    boolean authenticate,
                                                    Float failurePercentage,
                                                    Boolean failureOrder) throws IOException {
        final StringBuilder sb = new StringBuilder("Testing decryption of [%s] as full read of stream");

        if (failurePercentage != null && failureOrder != null) {
            sb.append("with ");
            sb.append(failureOrder ? "post" : "pre");
            sb.append("-read ");
            sb.append("failure at ");
            sb.append(failurePercentage);
        }

        System.out.printf(sb.append("%n").toString(),
                cipherDetails.getCipherId());

        canDecryptEntireObject(cipherDetails, SingleReads.class, authenticate, failurePercentage, failureOrder);
        canDecryptEntireObject(cipherDetails, ByteChunkReads.class, authenticate, failurePercentage, failureOrder);
        canDecryptEntireObject(cipherDetails, ByteChunkOffsetReads.class, authenticate, failurePercentage, failureOrder);
    }

    /**
     * Attempts to copy a {@link MantaEncryptedObjectInputStream} stream to a {@link java.io.OutputStream} and close the
     * streams. Copy is done using a large buffer size and logic borrowed directly from COSBench.
     */
    private void canCopyToOutputStreamWithLargeBuffer(SupportedCipherDetails cipherDetails,
                                                      boolean authenticate)
            throws IOException {
        final byte[] buffer = new byte[1024*1024];
        final int sourceLength = 8000;
        final byte[] sourceBytes = RandomUtils.nextBytes(sourceLength);

        SecretKey key = SecretKeyUtils.generate(cipherDetails);
        final EncryptedFile encryptedFile;

        try (InputStream in = new ByteArrayInputStream(sourceBytes)) {
            encryptedFile = encryptedFile(key, cipherDetails, sourceLength, in);
        }

        final byte[] iv = encryptedFile.cipher.getIV();

        final long ciphertextSize = encryptedFile.file.length();

        InputStream backing = new FileInputStream(encryptedFile.file);
        final InputStream inSpy = Mockito.spy(backing);

        MantaEncryptedObjectInputStream in = createEncryptedObjectInputStream(
                key, inSpy, ciphertextSize, cipherDetails, iv, authenticate,
                sourceLength);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try {
            // Don't change me - I'm imitating COSBench
            for (int n; -1 != (n = in.read(buffer));) {
                out.write(buffer, 0, n);
            }
        } finally {
            in.close();
            out.close();
        }

        AssertJUnit.assertArrayEquals(sourceBytes, out.toByteArray());
        Mockito.verify(inSpy, Mockito.atLeastOnce()).close();
    }

    /**
     * Test that loops through all of the ciphers and attempts to decrypt an encrypted stream that had its ciphertext
     * altered. An assertion fails if the underlying stream fails to throw a {@link
     * MantaClientEncryptionCiphertextAuthenticationException}. Additionally, this test tries to read data from the
     * stream using three different read methods and makes sure that all read methods function correctly.
     */
    private void willErrorIfCiphertextIsModifiedAllReadModes(SupportedCipherDetails cipherDetails) throws IOException {
        System.out.printf("Testing authentication of corrupted ciphertext with [%s] as full read of stream\n",
                cipherDetails.getCipherId());

        willThrowExceptionWhenCiphertextIsAltered(cipherDetails, SingleReads.class);
        willThrowExceptionWhenCiphertextIsAltered(cipherDetails, ByteChunkReads.class);
        willThrowExceptionWhenCiphertextIsAltered(cipherDetails, ByteChunkOffsetReads.class);
    }

    /**
     * Test that loops through all of the ciphers and attempts to decrypt an encrypted stream that had its ciphertext
     * altered. In this case, the stream is closed before all of the bytes are read from it. An assertion fails if the
     * underlying stream fails to throw a {@link MantaClientEncryptionCiphertextAuthenticationException}.
     */
    private void willErrorIfCiphertextIsModifiedAndNotReadFully(SupportedCipherDetails cipherDetails) throws IOException {
        System.out.printf("Testing authentication of corrupted ciphertext with [%s] as partial read of stream\n",
                cipherDetails.getCipherId());

        willThrowExceptionWhenCiphertextIsAltered(cipherDetails, SingleBytePartialRead.class);
    }

    /**
     * Test that loops through all of the ciphers and attempts to skip bytes from an encrypted stream. This test
     * verifies that the checksums are being calculated correctly even if bytes are skipped.
     */
    private void canSkipBytesAuthenticated(SupportedCipherDetails cipherDetails) throws IOException {
        System.out.printf("Testing authentication of ciphertext with [%s] as read and skips of stream\n",
                cipherDetails.getCipherId());

        canReadObject(cipherDetails, ReadAndSkipPartialRead.class, true);
    }

    /**
     * Test that loops through all of the ciphers and attempts to skip bytes from an encrypted stream.
     */
    private void canSkipBytesUnauthenticated(SupportedCipherDetails cipherDetails) throws IOException {
        System.out.printf("Testing authentication of ciphertext with [%s] as read and skips of stream\n",
                cipherDetails.getCipherId());

        canReadObject(cipherDetails, ReadAndSkipPartialRead.class, false);
    }

    private void canDecryptEntireObject(SupportedCipherDetails cipherDetails,
                                        Class<? extends ReadBytes> strategy,
                                        boolean authenticate,
                                        Float failurePercentage,
                                        Boolean failureOrder) throws IOException {
        SecretKey key = SecretKeyUtils.generate(cipherDetails);
        EncryptedFile encryptedFile = encryptedFile(key, cipherDetails, this.plaintextSize);
        long ciphertextSize = encryptedFile.file.length();

        final Pair<InputStream, InputStreamContinuator> in =
                buildEncryptedFileInputStream(encryptedFile, failurePercentage, failureOrder);


        final InputStream inSpy = Mockito.spy(in.getLeft());

        MantaEncryptedObjectInputStream min = createEncryptedObjectInputStream(key, inSpy,
                ciphertextSize, cipherDetails, encryptedFile.cipher.getIV(), authenticate,
                (long)this.plaintextSize);

        try {
            byte[] actual = new byte[plaintextSize];
            ReadBytesFactory.fullStrategy(strategy).readBytes(min, actual);

            AssertJUnit.assertArrayEquals("Plaintext doesn't match decrypted data", plaintextBytes, actual);
        } finally {
            min.close();
        }

        Mockito.verify(inSpy, Mockito.atLeastOnce()).close();
        verifyContinuatorWasUsedIfPresent(in);
    }

    /**
     * Test that attempts to skip bytes from an encrypted stream that had its ciphertext altered. In this case, the
     * stream is closed before all of the bytes are read from it. An assertion fails if the underlying stream fails to
     * throw a {@link MantaClientEncryptionCiphertextAuthenticationException}.
     */
    private void willErrorIfCiphertextIsModifiedAndBytesAreSkipped(SupportedCipherDetails cipherDetails) throws IOException {
        System.out.printf("Testing authentication of corrupted ciphertext with [%s] as read and skips of stream\n",
                cipherDetails.getCipherId());

        willThrowExceptionWhenCiphertextIsAltered(cipherDetails, ReadAndSkipPartialRead.class);
    }

    /**
     * Test that attempts to read a byte range from an encrypted stream starting at the first byte.
     */
    private void canReadByteRangeAllReadModes(SupportedCipherDetails cipherDetails,
                                              int startPosInclusive,
                                              int endPosInclusive)
            throws IOException {
        System.out.printf("Testing byte range read starting from zero for cipher [%s] as full read of "
                        + "truncated stream\n",
                cipherDetails.getCipherId());

        canReadByteRange(cipherDetails, SingleReads.class, startPosInclusive, endPosInclusive);
        canReadByteRange(cipherDetails, ByteChunkReads.class, startPosInclusive, endPosInclusive);
        canReadByteRange(cipherDetails, ByteChunkOffsetReads.class, startPosInclusive, endPosInclusive);
    }

    private void canReadByteRange(SupportedCipherDetails cipherDetails,
                                  Class<? extends ReadBytes> strategy,
                                  int startPosInclusive,
                                  int endPosInclusive) throws IOException {
        final byte[] content;
        try (InputStream in = testURL.openStream();
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            IOUtils.copy(in, out);
            content = out.toByteArray();
        }

        /* If the plaintext size specified is greater than the actual plaintext
         * size, we adjust it here. This is only for creating the expectation
         * that we compare our output to. */
        final int endPositionExclusive;

        if (endPosInclusive + 1 > plaintextSize) {
            endPositionExclusive = plaintextSize;
        } else {
            endPositionExclusive = endPosInclusive + 1;
        }

        boolean unboundedEnd = (endPositionExclusive >= plaintextSize || endPosInclusive < 0);
        byte[] expected = Arrays.copyOfRange(content, startPosInclusive, endPositionExclusive);

        SecretKey key = SecretKeyUtils.generate(cipherDetails);
        EncryptedFile encryptedFile = encryptedFile(key, cipherDetails, this.plaintextSize);
        long ciphertextSize = encryptedFile.file.length();

        /* Here we translate the plaintext ranges to ciphertext ranges. Notice
         * that we take the input of endPos because it may overrun past the
         * size of the plaintext. */
        ByteRangeConversion ranges = cipherDetails.translateByteRange(startPosInclusive, endPosInclusive);
        long ciphertextStart = ranges.getCiphertextStartPositionInclusive();
        long plaintextLength = ((long)(endPosInclusive - startPosInclusive)) + 1L;

        /* Here, we simulate being passed only a subset of the total bytes of
         * a file - just like how the output of a HTTP range request would be
         * passed. */
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(encryptedFile.file, "r")) {
            // We seek to the start of the ciphertext to emulate a HTTP byte range request
            randomAccessFile.seek(ciphertextStart);

            long initialSkipBytes = ranges.getPlaintextBytesToSkipInitially() + ranges.getCiphertextStartPositionInclusive();
            long binaryEndPositionInclusive = ranges.getCiphertextEndPositionInclusive();

            long ciphertextRangeMaxBytes = ciphertextSize - ciphertextStart;
            long ciphertextRangeRequestBytes = binaryEndPositionInclusive - ciphertextStart + 1;

            /* We then calculate the range length based on the *actual* ciphertext
             * size so that we are emulating HTTP range requests by returning a
             * the binary of the actual object (even if the range went beyond). */
            long ciphertextByteRangeLength = Math.min(ciphertextRangeMaxBytes, ciphertextRangeRequestBytes);

            if (unboundedEnd)
                ciphertextByteRangeLength = ciphertextSize - ciphertextStart; // include MAC

            byte[] rangedBytes = new byte[(int)ciphertextByteRangeLength];
            randomAccessFile.read(rangedBytes);

            // it's a byte array, close does nothing so skip try-with-resources
            final ByteArrayInputStream bin = new ByteArrayInputStream(rangedBytes);
            final ByteArrayInputStream binSpy = Mockito.spy(bin);

            /* When creating the fake stream, we feed it a content-length equal
             * to the size of the byte range because that is what Manta would
             * do. Also, we pass along the incorrect plaintext length and
             * test how the stream handles incorrect values of plaintext length. */
             MantaEncryptedObjectInputStream min = createEncryptedObjectInputStream(key, binSpy,
                     ciphertextByteRangeLength, cipherDetails, encryptedFile.cipher.getIV(),
                false, (long)this.plaintextSize,
                     initialSkipBytes,
                     plaintextLength,
                     unboundedEnd);

            byte[] actual = new byte[expected.length];
            ReadBytesFactory.fullStrategy(strategy).readBytes(min, actual);

            min.close();
            Mockito.verify(binSpy, Mockito.atLeastOnce()).close();

            try {
                AssertJUnit.assertArrayEquals("Byte range output doesn't match",
                        expected, actual);
            } catch (AssertionError e) {
                Assert.fail(String.format("%s\nexpected: %s\nactual  : %s",
                        e.getMessage(),
                        new String(expected, StandardCharsets.UTF_8),
                        new String(actual, StandardCharsets.UTF_8)));
            }
        }
    }

    private void canReadObject(SupportedCipherDetails cipherDetails,
                               Class<? extends ReadBytes> strategy,
                               boolean authenticate) throws IOException {
        SecretKey key = SecretKeyUtils.generate(cipherDetails);
        EncryptedFile encryptedFile = encryptedFile(key, cipherDetails, this.plaintextSize);
        long ciphertextSize = encryptedFile.file.length();

        final FileInputStream in = new FileInputStream(encryptedFile.file);
        final FileInputStream inSpy = Mockito.spy(in);
        MantaEncryptedObjectInputStream min = createEncryptedObjectInputStream(key, inSpy,
                ciphertextSize, cipherDetails, encryptedFile.cipher.getIV(),
                authenticate, (long)this.plaintextSize);

        try {
            byte[] actual = new byte[plaintextSize];
            ReadBytesFactory.build(strategy, encryptedFile.file.length()).readBytes(min, actual);
        } finally {
            min.close();
            Mockito.verify(inSpy, Mockito.atLeastOnce()).close();
        }
    }

    private void willThrowExceptionWhenCiphertextIsAltered(SupportedCipherDetails cipherDetails,
                                                           Class<? extends ReadBytes> strategy)
            throws IOException {
        SecretKey key = SecretKeyUtils.generate(cipherDetails);
        EncryptedFile encryptedFile = encryptedFile(key, cipherDetails, this.plaintextSize);

        try (FileChannel fc = (FileChannel.open(encryptedFile.file.toPath(), READ, WRITE))) {
            fc.position(2);
            ByteBuffer buff = ByteBuffer.wrap(new byte[] { 20, 20 });
            fc.write(buff);
        }

        long ciphertextSize = encryptedFile.file.length();

        boolean thrown = false;

        FileInputStream in = new FileInputStream(encryptedFile.file);
        final FileInputStream inSpy = Mockito.spy(in);

        MantaEncryptedObjectInputStream min = createEncryptedObjectInputStream(key, inSpy,
                ciphertextSize, cipherDetails, encryptedFile.cipher.getIV(),
                true, (long)this.plaintextSize);

        try {
            byte[] actual = new byte[plaintextSize];
            ReadBytesFactory.build(strategy, encryptedFile.file.length()).readBytes(min, actual);
            min.close();
        }  catch (MantaClientEncryptionCiphertextAuthenticationException e) {
            thrown = true;
            Mockito.verify(inSpy, Mockito.atLeastOnce()).close();
        }

        Assert.assertTrue(thrown, "Ciphertext authentication exception wasn't thrown");
    }

    private void willErrorWhenMissingHMAC(final SupportedCipherDetails cipherDetails) throws IOException {
        SecretKey key = SecretKeyUtils.generate(cipherDetails);
        EncryptedFile encryptedFile = encryptedFile(key, cipherDetails, this.plaintextSize);
        long ciphertextSize = encryptedFile.file.length();
        int hmacSize = cipherDetails.getAuthenticationTagOrHmacLengthInBytes();
        long ciphertextSizeWithoutHmac = ciphertextSize - hmacSize;

        boolean thrown = false;

        final FileInputStream fin = new FileInputStream(encryptedFile.file);
        final FileInputStream finSpy = Mockito.spy(fin);

        try (BoundedInputStream in = new BoundedInputStream(finSpy, ciphertextSizeWithoutHmac);
             MantaEncryptedObjectInputStream min = createEncryptedObjectInputStream(key, in,
                     ciphertextSize, cipherDetails, encryptedFile.cipher.getIV(),
                     true, (long)this.plaintextSize)) {
            // Do a single read to make sure that everything is working
            Assert.assertNotEquals(min.read(), -1,
                    "The encrypted stream should not be empty");
        } catch (MantaIOException e) {
            if (e.getMessage().startsWith("No HMAC was stored at the end of the stream")) {
                thrown = true;
            } else {
                throw e;
            }
        }

        Mockito.verify(finSpy, Mockito.atLeastOnce()).close();
        Assert.assertTrue(thrown, "Expected MantaIOException was not thrown");
    }

    private void willValidateIfHmacIsReadInMultipleReads(final SupportedCipherDetails cipherDetails)
            throws IOException {
        SecretKey key = SecretKeyUtils.generate(cipherDetails);
        EncryptedFile encryptedFile = encryptedFile(key, cipherDetails, this.plaintextSize);
        long ciphertextSize = encryptedFile.file.length();


        final FileInputStream fin = new FileInputStream(encryptedFile.file);
        final FileInputStream finSpy = Mockito.spy(fin);

        try (IncompleteByteReadInputStream ibrin = new IncompleteByteReadInputStream(finSpy);
             MantaEncryptedObjectInputStream min = createEncryptedObjectInputStream(key, ibrin,
                     ciphertextSize, cipherDetails, encryptedFile.cipher.getIV(),
                     true, (long)this.plaintextSize);
             OutputStream out = new NullOutputStream()) {

            IOUtils.copy(min, out);
        }

        Mockito.verify(finSpy, Mockito.atLeastOnce()).close();
    }

    private EncryptedFile encryptedFile(
            SecretKey key, SupportedCipherDetails cipherDetails,
            long plaintextSize) throws IOException {
        File temp = File.createTempFile("encrypted", ".data");
        FileUtils.forceDeleteOnExit(temp);

        try (InputStream in = testURL.openStream()) {
            return encryptedFile(key, cipherDetails, plaintextSize, in);
        }
    }

    private EncryptedFile encryptedFile(
            SecretKey key, SupportedCipherDetails cipherDetails, long plaintextSize,
            InputStream in) throws IOException {
        File temp = File.createTempFile("encrypted", ".data");
        FileUtils.forceDeleteOnExit(temp);

        try (FileOutputStream out = new FileOutputStream(temp)) {
            MantaInputStreamEntity entity = new MantaInputStreamEntity(in, plaintextSize);
            EncryptingEntity encryptingEntity = new EncryptingEntity(
                    key, cipherDetails, entity);
            encryptingEntity.writeTo(out);

            Assert.assertEquals(temp.length(), encryptingEntity.getContentLength(),
                    "Ciphertext doesn't equal calculated size");

            return new EncryptedFile(encryptingEntity.getCipher(), temp);
        }
    }

    private MantaEncryptedObjectInputStream createEncryptedObjectInputStream(
            SecretKey key, InputStream in, long contentLength,
            SupportedCipherDetails cipherDetails,
            byte[] iv, boolean authenticate, long plaintextSize) {
        return createEncryptedObjectInputStream(key, in, contentLength, cipherDetails, iv,
                authenticate, plaintextSize, null, null, true);
    }

    private MantaEncryptedObjectInputStream createEncryptedObjectInputStream(
            SecretKey key, InputStream in, long ciphertextSize, SupportedCipherDetails cipherDetails,
            byte[] iv, boolean authenticate, long plaintextSize, Long skipBytes, Long plaintextLength, boolean unboundedEnd) {
        String path = String.format("/test/stor/test-%s", UUID.randomUUID());
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.put(MantaHttpHeaders.ENCRYPTION_CIPHER, cipherDetails.getCipherId());
        headers.put(MantaHttpHeaders.ENCRYPTION_PLAINTEXT_CONTENT_LENGTH, plaintextSize);
        headers.put(MantaHttpHeaders.ENCRYPTION_KEY_ID, "unit-test-key");

        if (cipherDetails.isAEADCipher()) {
            headers.put(MantaHttpHeaders.ENCRYPTION_AEAD_TAG_LENGTH, cipherDetails.getAuthenticationTagOrHmacLengthInBytes());
        } else {
            final String hmacName = SupportedHmacsLookupMap.hmacNameFromInstance(
                    cipherDetails.getAuthenticationHmac());
            headers.put(MantaHttpHeaders.ENCRYPTION_HMAC_TYPE, hmacName);
        }

        headers.put(MantaHttpHeaders.ENCRYPTION_IV,
                Base64.getEncoder().encodeToString(iv));

        headers.setContentLength(ciphertextSize);

        MantaMetadata metadata = new MantaMetadata();

        MantaObjectResponse response = new MantaObjectResponse(path, headers, metadata);

        CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);

        EofSensorInputStream eofSensorInputStream = new EofSensorInputStream(in, null);

        MantaObjectInputStream mantaObjectInputStream = new MantaObjectInputStream(
                response, httpResponse, eofSensorInputStream
        );

        return new MantaEncryptedObjectInputStream(mantaObjectInputStream,
                cipherDetails, key, authenticate,
                skipBytes, plaintextLength, unboundedEnd);
    }


    //@formatter:off
    /**
     *
     * Builds an {@link InputStream} from the supplied file and returns it. In case a failure percentage and order are
     * provided, we will:
     *  - wrap the first stream in a {@link FailingInputStream}
     *  - build an {@link InputStreamContinuator} which can provide streams for the remaining data
     *  - build an {@link AutoContinuingInputStream} which uses the continuator to recover from the initial failure
     *
     * We need to return both the requested {@link InputStream} and (if on exists) the {@link InputStreamContinuator}
     * so that the test can check that the continuator was actually used.
     */
    //@formatter:on
    private static Pair<InputStream, InputStreamContinuator> buildEncryptedFileInputStream(final EncryptedFile encryptedFile,
                                                                                           final Float failurePercent,
                                                                                           final Boolean failureOrder)
            throws FileNotFoundException {
        if (failurePercent == null || failureOrder == null) {
            return ImmutablePair.of(new FileInputStream(encryptedFile.file), null);
        }

        final InputStream initialStream = new FailingInputStream(new FileInputStream(encryptedFile.file),
                                                                 Math.round(
                                                                         encryptedFile.file.length() * failurePercent),
                                                                 failureOrder);
        final InputStreamContinuator continuator = Mockito.spy(new FileInputStreamContinuator(encryptedFile.file));

        return ImmutablePair.of(new AutoContinuingInputStream(initialStream, continuator), continuator);
    }

    /**
     * In case a test injected a failure, it would have also build a continuator to recover from that failure. If we
     * see a continuator, we verify it was used.
     */
    private void verifyContinuatorWasUsedIfPresent(final Pair<InputStream, InputStreamContinuator> inputs) throws IOException {
        if (inputs.getRight() != null) {
            Mockito.verify(inputs.getRight()).buildContinuation(Mockito.any(IOException.class), Mockito.anyLong());
        }
    }
}
