/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.exception.MantaClientEncryptionException;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.commons.lang3.CharUtils;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Utility methods for performing operations related to encrypted metadata
 * on {@link MantaMetadata}.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public final class EncryptedMetadataUtils {
    /**
     * Private constructor for utility class.
     */
    private EncryptedMetadataUtils() {
    }

    /**
     * Converts the headers to be encrypted to a plaintext string. This returns
     * the value of that will be encrypted and stored as ciphertext.
     *
     * @param metadata metadata object containing items to be encrypted
     * @return string containing headers in the format of <code>Header: Value</code>
     */
    public static String encryptedMetadataAsString(final MantaMetadata metadata) {
        Set<Map.Entry<String, String>> entrySet = metadata.entrySet();
        Iterator<Map.Entry<String, String>> iterator = entrySet.iterator();
        StringBuilder builder = new StringBuilder();

        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();

            if (entry.getKey().startsWith(MantaMetadata.ENCRYPTED_METADATA_PREFIX)) {
                String line = String.format("%s: %s", entry.getKey(), entry.getValue());
                builder.append(line);

                if (iterator.hasNext()) {
                    builder.append(CharUtils.LF);
                }
            }
        }

        return builder.toString();
    }

    /**
     * Parses a plaintext metadata string and converts it into a {@link Map} of
     * keys and values.
     *
     * @param plaintext Plaintext string in US-ASCII encoding
     * @return headers as map
     */
    public static Map<String, String> plaintextMetadataAsMap(final String plaintext) {
        return plaintextMetadataAsMap(plaintext.getBytes(StandardCharsets.US_ASCII));
    }

    /**
     * Parses a plaintext metadata string and converts it into a {@link Map} of
     * keys and values.
     *
     * @param plaintext Plaintext binary data in US-ASCII encoding
     * @return headers as map
     */
    public static Map<String, String> plaintextMetadataAsMap(final byte[] plaintext) {
        Map<String, String> map = new CaseInsensitiveMap<>();

        boolean parsingKey = true;
        boolean parsingVal = false;

        final int initialSize = 24;
        StringBuilder key = new StringBuilder(initialSize);
        StringBuilder val = new StringBuilder(initialSize);

        for (int i = 0; i <= plaintext.length; i++) {
            // Done parsing a line, now we add it to the map
            if (i == plaintext.length || (char)plaintext[i] == CharUtils.LF) {
                if (key.length() > 0) {
                    map.put(key.toString(), val.toString());
                }

                key.setLength(0);
                val.setLength(0);
                parsingKey = true;
                parsingVal = false;
                continue;
            }

            char c = (char)plaintext[i];

            if (!CharUtils.isAsciiPrintable(c)) {
                String msg = "Encrypted metadata contained a "
                        + "non-ascii or unprintable character";
                throw new MantaClientEncryptionException(msg);
            }

            if (c == ':' && parsingKey) {
                parsingKey = false;
                continue;
            }

            if (c == ' ' && !parsingKey && !parsingVal) {
                continue;
            } else if (!parsingKey && !parsingVal) {
                parsingVal = true;
            }

            if (parsingKey) {
                key.append(c);
            }

            if (parsingVal) {
                val.append(c);
            }
        }

        return map;
    }
}
