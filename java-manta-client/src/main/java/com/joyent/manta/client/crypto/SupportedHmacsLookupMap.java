/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import com.joyent.manta.exception.MantaClientEncryptionException;
import com.twmacinta.util.FastMD5Digest;
import com.joyent.manta.util.LookupMap;
import com.joyent.manta.util.MantaUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.bouncycastle.crypto.digests.SHA1Digest;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.digests.SHA384Digest;
import org.bouncycastle.crypto.digests.SHA512Digest;
import org.bouncycastle.crypto.macs.HMac;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Custom built {@link Map} implementation that supports case-sensitive and
 * case-insensitive operations for looking up {@link javax.crypto.Mac}
 * instances by algorithm name.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public final class SupportedHmacsLookupMap extends LookupMap<String, Supplier<HMac>> {
    /**
     * Map of all of the HMACs supported by the SDK indexed by algorithm name.
     */
    public static final SupportedHmacsLookupMap INSTANCE = new SupportedHmacsLookupMap();

    /**
     * Private constructor because interface is through {@link SupportedCipherDetails}.
     */
    private SupportedHmacsLookupMap() {
        super(MantaUtils.unmodifiableMap(
                "HmacMD5", hmacSupplierByName("HmacMD5"),
                "HmacSHA1", hmacSupplierByName("HmacSHA1"),
                "HmacSHA256", hmacSupplierByName("HmacSHA256"),
                "HmacSHA384", hmacSupplierByName("HmacSHA384"),
                "HmacSHA512", hmacSupplierByName("HmacSHA512"))
        );
    }

    /**
     * Wraps a getInstance call as a {@link Supplier} so that we can return a
     * new HMAC instance for every value of this map.
     *
     * @param algorithm algorithm to instantiate HMAC instance as
     * @return supplier wrapping getInstance call to get HMAC instance
     */
    private static Supplier<HMac> hmacSupplierByName(final String algorithm) {
        return () -> {
            if (algorithm.equalsIgnoreCase("HmacMD5")) {
                return new HMac(new FastMD5Digest());
            } else if (algorithm.equalsIgnoreCase("HmacSHA1")) {
                return new HMac(new SHA1Digest());
            } else if (algorithm.equalsIgnoreCase("HmacSHA256")) {
                return new HMac(new SHA256Digest());
            } else if (algorithm.equalsIgnoreCase("HmacSHA384")) {
                return new HMac(new SHA384Digest());
            } else if (algorithm.equalsIgnoreCase("HmacSHA512")) {
                return new HMac(new SHA512Digest());
            } else {
                String msg = String.format("Hmac algorithm [%s] not supported",
                        algorithm);
                throw new MantaClientEncryptionException(msg);
            }
        };
    }

    /**
     * Finds the HMAC implementation name based on the passed object.
     *
     * @param hmac instance to find name for
     * @return the name of implementation used by the SDK
     */
    public static String hmacNameFromInstance(final HMac hmac) {
        Validate.notNull(hmac, "HMAC instance must not be null");

        final String digestName = hmac.getUnderlyingDigest().getAlgorithmName();
        return "Hmac" + StringUtils.strip(digestName, "-");
    }
}
