/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.crypto.SupportedHmacsLookupMap;
import com.joyent.manta.client.multipart.AbstractMultipartUpload;
import com.joyent.manta.client.multipart.EncryptedMultipartUpload;
import com.joyent.manta.exception.MantaClientEncryptionCiphertextAuthenticationException;
import com.joyent.manta.exception.MantaClientEncryptionException;
import org.bouncycastle.crypto.macs.HMac;
import org.bouncycastle.util.Arrays;
import org.bouncycastle.util.encoders.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.function.Supplier;

/**
 * Helper class that acts as the central point of entry for
 * serializing and encrypting {@link EncryptedMultipartUpload}
 * instances.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 *
 * @param <WRAPPED> backing implementation of a {@link AbstractMultipartUpload}
 */
public class EncryptedMultipartUploaSerializationHelper<WRAPPED extends AbstractMultipartUpload> {
    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(EncryptedMultipartUploaSerializationHelper.class);

    /**
     * Size in bytes of binary record identifying HMAC algorithm name.
     */
    private static final int CIPHER_ID_SIZE_BYTES = 16;

    /**
     * Serialized object graph version.
     */
    private static final int ENCRYPTED_MULTIPART_UPLOAD_SERIALIZATION_VERSION = 1;

    /**
     * Kryo serializer instance.
     */
    private final Kryo kryo;

    /**
     * The secret key used in the encrypted MPU and used to encrypt with.
     */
    private final SecretKey secretKey;

    /**
     * The cipher/mode used to encrypt the serialized data with.
     */
    private final SupportedCipherDetails cipherDetails;

    /**
     * Multipart class that encrypting mpu class is wrapping.
     */
    private final Class<WRAPPED> wrappedMultipartClass;

    /**
     * Creates a new instance.
     *
     * @param kryo Kryo serializer instance
     * @param secretKey secret key used by MPU
     * @param cipherDetails cipher / mode properties object
     * @param wrappedMultipartClass multipart class that encrypting mpu class is wrapping
     */
    public EncryptedMultipartUploaSerializationHelper(final Kryo kryo,
                                                      final SecretKey secretKey,
                                                      final SupportedCipherDetails cipherDetails,
                                                      final Class<WRAPPED> wrappedMultipartClass) {
        this.kryo = kryo;
        this.secretKey = secretKey;
        this.wrappedMultipartClass = wrappedMultipartClass;
        this.cipherDetails = cipherDetails;

        registerClasses();
    }

    /**
     * Registers the classes needed for serialization with Kryo.
     */
    private void registerClasses() {
        kryo.register(EncryptedMultipartUpload.class,
                new EncryptedMultipartSerializer<>(kryo, EncryptedMultipartUpload.class,
                        wrappedMultipartClass, secretKey));
    }

    /**
     * Serializes and encrypts the specified upload object.
     *
     * @param upload object to serialize and encrypt
     * @return serialized and encrypted byte array
     */
    public byte[] serialize(final EncryptedMultipartUpload<WRAPPED> upload) {
        Cipher cipher = cipherDetails.getCipher();
        byte[] iv = cipherDetails.generateIv();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Writing IV: {}", Hex.toHexString(iv));
        }

        try {
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, cipherDetails.getEncryptionParameterSpec(iv));
        } catch (GeneralSecurityException e) {
            String msg = String.format("Unable to initialize cipher [%s]",
                    cipherDetails.getCipherId());
            throw new MantaClientEncryptionException(msg, e);
        }

        final byte[] serializedData;
        // 16k buffer *should* handle the serialized content
        final int outputBufferSize = 8192;
        final int maxBufferSize = 16384;
        try (Output output = new ByteBufferOutput(outputBufferSize, maxBufferSize)) {
            output.writeVarInt(ENCRYPTED_MULTIPART_UPLOAD_SERIALIZATION_VERSION, true);
            kryo.writeClassAndObject(output, upload);
            output.flush();
            serializedData = output.toBytes();
        }

        final byte[] cipherText;

        try {
             cipherText = cipher.doFinal(serializedData);
        } catch (GeneralSecurityException e) {
            String msg = "Error encrypting serialized data";
            throw new MantaClientEncryptionException(msg, e);
        }

        // Authentication HMAC
        if (!cipherDetails.isAEADCipher()) {
            HMac hmac = cipherDetails.getAuthenticationHmac();
            hmac.update(iv, 0, iv.length);
            hmac.update(cipherText, 0, cipherText.length);
            final byte[] checksum = new byte[hmac.getMacSize()];
            hmac.doFinal(checksum, 0);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Writing HMAC: {}", Hex.toHexString(checksum));
            }

            final byte[] hmacId = new byte[CIPHER_ID_SIZE_BYTES];
            final String hmacIdString = SupportedHmacsLookupMap.hmacNameFromInstance(hmac);
            final byte[] hmacIdStringBytes = hmacIdString.getBytes(StandardCharsets.US_ASCII);
            System.arraycopy(hmacIdStringBytes, 0, hmacId, 0, hmacIdStringBytes.length);

            return addAll(iv, cipherText, checksum, hmacId);
        }

        return addAll(iv, cipherText);
    }

    /**
     * Decrypts and deserializes the specified binary blob.
     *
     * @param serializedData data to decrypt and deserialize
     * @return an upload object
     */
    @SuppressWarnings("unchecked")
    public EncryptedMultipartUpload<WRAPPED> deserialize(final byte[] serializedData) {
        final byte[] iv = Arrays.copyOf(serializedData, cipherDetails.getIVLengthInBytes());

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Reading IV: {}", Hex.toHexString(iv));
        }

        final Cipher cipher = cipherDetails.getCipher();
        try {
            cipher.init(Cipher.DECRYPT_MODE, secretKey, cipherDetails.getEncryptionParameterSpec(iv));
        } catch (GeneralSecurityException e) {
            String msg = String.format("Unable to initialize cipher [%s]",
                    cipherDetails.getCipherId());
            throw new MantaClientEncryptionException(msg, e);
        }

        final byte[] cipherText;

        if (cipherDetails.isAEADCipher()) {
            cipherText = extractCipherText(serializedData, iv.length, null);
        } else {
            final byte[] hmacIdBytes = Arrays.copyOfRange(serializedData,
                    serializedData.length - CIPHER_ID_SIZE_BYTES, serializedData.length);
            final String hmacId = new String(hmacIdBytes, StandardCharsets.US_ASCII).trim();

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Verifying checksum with [{}]", hmacId);
            }

            final Supplier<HMac> hmacSupplier = SupportedHmacsLookupMap.INSTANCE.get(hmacId);

            if (hmacSupplier == null) {
                String msg = String.format("Unknown HMAC: [%s]", hmacId);
                throw new MantaClientEncryptionException(msg);
            }

            final HMac hmac = hmacSupplier.get();
            final int hmacLength = hmac.getMacSize();

            cipherText = extractCipherText(serializedData, iv.length, hmacLength);

            hmac.update(iv, 0, iv.length);
            hmac.update(cipherText, 0, cipherText.length);
            final byte[] calculated = new byte[hmacLength];
            hmac.doFinal(calculated, 0);

            final byte[] expected = Arrays.copyOfRange(serializedData,
                    serializedData.length - hmacLength - CIPHER_ID_SIZE_BYTES,
                    serializedData.length - CIPHER_ID_SIZE_BYTES);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Expected HMAC:   {}", Hex.toHexString(expected));
                LOGGER.debug("Calculated HMAC: {}", Hex.toHexString(calculated));
            }

            if (!Arrays.areEqual(calculated, expected)) {
                String msg = "Serialization data ciphertext failed "
                        + "cryptographic authentication";
                MantaClientEncryptionCiphertextAuthenticationException e =
                        new MantaClientEncryptionCiphertextAuthenticationException(msg);
                e.setContextValue("expected", Hex.toHexString(expected));
                e.setContextValue("calculated", Hex.toHexString(calculated));
                throw e;
            }
        }

        final byte[] plaintext;
        try {
            plaintext = cipher.doFinal(cipherText);
        } catch (GeneralSecurityException e) {
            String msg = "Error decrypting serialized object data";
            throw new MantaClientEncryptionException(msg, e);
        }

        final EncryptedMultipartUpload<WRAPPED> upload;

        try (Input input = new Input(plaintext)) {
            final int serializationVersion = input.readVarInt(true);

            if (serializationVersion != ENCRYPTED_MULTIPART_UPLOAD_SERIALIZATION_VERSION) {
                LOGGER.warn("Deserialized version [%d] is different than serialization version [%d",
                        serializationVersion, ENCRYPTED_MULTIPART_UPLOAD_SERIALIZATION_VERSION);
            }

            upload = (EncryptedMultipartUpload<WRAPPED>)kryo.readClassAndObject(input);
        }

        return upload;
    }

    /**
     * Extracts the cipher text value from a byte array.
     *
     * @param serializedData byte array to extract ciphertext from
     * @param ivLength size of IV
     * @param hmacLength size of HMAC (or null for AEAD ciphers)
     * @return byte array containing only ciphertext
     */
    private byte[] extractCipherText(final byte[] serializedData,
                                     final int ivLength,
                                     final Integer hmacLength) {
        if (hmacLength == null) {
            return Arrays.copyOfRange(serializedData, ivLength,
                    serializedData.length);
        } else {
            return Arrays.copyOfRange(serializedData, ivLength,
                    serializedData.length - hmacLength - CIPHER_ID_SIZE_BYTES);
        }
    }


    /**
     * Concatenates N number of byte arrays together.
     *
     * @param byteArrays byte arrays to concatenate
     * @return a single byte array composed of all of the passed arrays joined together
     */
    static byte[] addAll(final byte[]... byteArrays) {
        int joinedArraySize = 0;

        for (byte[] array : byteArrays) {
            if (array == null) {
                continue;
            }
            joinedArraySize += array.length;
        }

        final byte[] joinedArray = new byte[joinedArraySize];

        int position = 0;

        for (byte[] array : byteArrays) {
            if (array == null) {
                continue;
            }

            System.arraycopy(array, 0, joinedArray, position, array.length);
            position += array.length;
        }

        return joinedArray;
    }
}
