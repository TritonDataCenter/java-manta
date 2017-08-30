/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.http.signature.KeyFingerprinter;
import com.joyent.http.signature.ThreadLocalSigner;
import com.joyent.manta.config.ConfigContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.bouncycastle.util.encoders.Base64;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;

/**
 * Class used to create signed URLs using the Manta-compatible HTTP signature
 * method.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class UriSigner {
    /**
     * Manta configuration object.
     */
    private final ConfigContext config;

    /**
     * HTTP signature generator instance.
     */
    private final ThreadLocalSigner signer;

    /**
     * Cryptographic key pair used to sign URIs.
     */
    private final KeyPair keyPair;

    /**
     * Creates a new instance.
     *
     * @param config Manta configuration context
     * @param keyPair cryptographic key pair used to sign URIs
     * @param signer Signer configured to work with the the given keyPair
     */
    public UriSigner(final ConfigContext config, final KeyPair keyPair, final ThreadLocalSigner signer) {
        this.config = config;
        this.keyPair = keyPair;
        this.signer = signer;
    }

    /**
     * Signs an arbitrary URL using the Manta-compatible HTTP signature
     * method.
     *
     * @param uri URI with no query pointing to a downloadable resource
     * @param method HTTP request method to be used in the signature
     * @param expires epoch time in seconds when the resource will no longer
     *                be available
     * @return a signed version of the input URI
     * @throws IOException thrown when we can't sign or read char data
     */
    public URI signURI(final URI uri, final String method, final long expires)
            throws IOException {
        Validate.notNull(method, "Method must not be null");
        Validate.notNull(uri, "URI must not be null");

        if (uri.getQuery() != null) {
            Validate.isTrue(StringUtils.isEmpty(uri.getQuery()),
                    "Query must be null or empty. URI: %s", uri);
        }

        final String charset = "UTF-8";
        final String algorithm = signer.get().getHttpHeaderAlgorithm().toUpperCase();
        final String keyId = String.format("/%s/keys/%s",
                                           config.getMantaUser(),
                                           KeyFingerprinter.md5Fingerprint(keyPair));

        final String keyIdEncoded = URLEncoder.encode(keyId, charset);

        StringBuilder sigText = new StringBuilder();
        sigText.append(method).append(StringUtils.LF)
                .append(uri.getHost()).append(StringUtils.LF)
                .append(uri.getPath()).append(StringUtils.LF)
                .append("algorithm=").append(algorithm).append("&")
                .append("expires=").append(expires).append("&")
                .append("keyId=").append(keyIdEncoded);


        StringBuilder request = new StringBuilder();
        final byte[] sigBytes = sigText.toString().getBytes(
                StandardCharsets.UTF_8);
        final byte[] signed = signer.get().sign(config.getMantaUser(),
                                                keyPair, sigBytes);
        final String encoded = new String(Base64.encode(signed), charset);
        final String urlEncoded = URLEncoder.encode(encoded, charset);

        request.append(uri).append("?")
                .append("algorithm=").append(algorithm).append("&")
                .append("expires=").append(expires).append("&")
                .append("keyId=").append(keyIdEncoded).append("&")
                .append("signature=").append(urlEncoded);

        return URI.create(request.toString());
    }
}
