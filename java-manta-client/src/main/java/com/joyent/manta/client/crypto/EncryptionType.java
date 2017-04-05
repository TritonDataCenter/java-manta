/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import com.joyent.manta.exception.MantaEncryptionException;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static com.joyent.manta.util.MantaVersion.CLIENT_SIDE_ENCRYPTION_MAX_VERSION;
import static com.joyent.manta.util.MantaVersion.CLIENT_SIDE_ENCRYPTION_MIN_VERSION;

/**
 * Enum listing all of the encryption type supported in the SDK.
 * Currently, only "client" is supported.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public final class EncryptionType {
    /**
     * Client-side encryption mode.
     */
    public static final EncryptionType CLIENT = new EncryptionType(
            "client",
            CLIENT_SIDE_ENCRYPTION_MIN_VERSION,
            CLIENT_SIDE_ENCRYPTION_MAX_VERSION);

    /**
     * Map of supported encryption types index by name.
     */
    private static final Map<String, EncryptionType> SUPPORTED_ENCRYPTION_TYPES =
            Collections.singletonMap(CLIENT.name, CLIENT);

    /**
     * Separator string between type name and version.
     */
    private static final String SEPARATOR = "/";

    /**
     * Encryption type name.
     */
    private final String name;

    /**
     * The minimum version of the encryption type supported.
     */
    private final int minVersionSupported;

    /**
     * The maximum version of the encryption type supported.
     */
    private final int maxVersionSupported;

    /**
     * The full identifier (type/version).
     */
    private final String id;

    /**
     * Creates a new private instance of an encryption type.
     *
     * @param name name of encryption type
     * @param minVersionSupported minimum version supported.
     * @param maxVersionSupported maximum version supported.
     */
    private EncryptionType(final String name,
                           final int minVersionSupported,
                           final int maxVersionSupported) {
        this.name = name;
        this.minVersionSupported = minVersionSupported;
        this.maxVersionSupported = maxVersionSupported;
        this.id = String.format("%s%s%d", name, SEPARATOR, maxVersionSupported);
    }

    /**
     * Validates that the encryption type and version is compatible with the SDK.
     * @param encryptionType string of encryption type and version to parse and validate
     * @throws MantaEncryptionException when encryption type can't be validated
     */
    public static void validateEncryptionTypeIsSupported(final String encryptionType) {
        if (encryptionType == null) {
            String msg = "Invalid encryption type identifier must not be null";
            throw new MantaEncryptionException(msg);
        }

        if (StringUtils.isBlank(encryptionType)) {
            String msg = "Invalid encryption type identifier must not be blank";
            MantaEncryptionException e = new MantaEncryptionException(msg);
            e.setContextValue("malformedEncryptionType", String.format("[%s]", encryptionType));
            throw e;
        }

        final String[] parts = encryptionType.split(SEPARATOR, 2);

        if (parts.length != 2) {
            String msg = "Invalid encryption type identifier specified: missing version separator.";
            MantaEncryptionException e = new MantaEncryptionException(msg);
            e.setContextValue("malformedEncryptionType", encryptionType);
            throw e;
        }

        final EncryptionType type = SUPPORTED_ENCRYPTION_TYPES.get(parts[0]);

        if (type == null) {
            String msg = "Invalid encryption type identifier specified: Unknown type.";
            MantaEncryptionException e = new MantaEncryptionException(msg);
            e.setContextValue("malformedEncryptionType", encryptionType);
            e.setContextValue("type", parts[0]);
            throw e;
        }

        final int version;

        try {
            version = Integer.parseUnsignedInt(parts[1]);
        } catch (NumberFormatException e) {
            String msg = "Invalid encryption type version identifier specified.";
            MantaEncryptionException mcee = new MantaEncryptionException(msg, e);
            mcee.setContextValue("malformedEncryptionType", encryptionType);
            mcee.setContextValue("malformedVersionString", parts[1]);
            throw mcee;
        }

        if (version > type.maxVersionSupported) {
            String msg = "Encryption type version is greater than supported";
            MantaEncryptionException e = new MantaEncryptionException(msg);
            e.setContextValue("encryptionType", encryptionType);
            e.setContextValue("desiredVersion", version);
            e.setContextValue("minimumVersionSupported", type.minVersionSupported);
            e.setContextValue("maximumVersionSupported", type.maxVersionSupported);
            throw e;
        }

        if (version < type.maxVersionSupported) {
            String msg = "Encryption type version is less than supported";
            MantaEncryptionException e = new MantaEncryptionException(msg);
            e.setContextValue("encryptionType", encryptionType);
            e.setContextValue("desiredVersion", version);
            e.setContextValue("minimumVersionSupported", type.minVersionSupported);
            e.setContextValue("maximumVersionSupported", type.maxVersionSupported);
            throw e;
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final EncryptionType that = (EncryptionType) o;

        return minVersionSupported == that.minVersionSupported
            && maxVersionSupported == that.maxVersionSupported
            && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, minVersionSupported, maxVersionSupported);
    }

    @Override
    public String toString() {
        return this.id;
    }
}
