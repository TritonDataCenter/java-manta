package com.joyent.manta.client;

import com.joyent.http.signature.ThreadLocalSigner;
import com.joyent.manta.config.ConfigContext;
import org.bouncycastle.util.encoders.Base64;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.security.KeyPair;
import java.util.Objects;

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
    private final ThreadLocalSigner signer = new ThreadLocalSigner();

    /**
     * Cryptographic key pair used to sign URIs.
     */
    private final KeyPair keyPair;

    /**
     * Creates a new instance.
     *
     * @param config Manta configuration context
     * @param keyPair cryptographic key pair used to sign URIs
     */
    public UriSigner(final ConfigContext config, final KeyPair keyPair) {
        this.config = config;
        this.keyPair = keyPair;
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
        Objects.requireNonNull(method, "Method must be present");
        Objects.requireNonNull(uri, "URI must be present");

        if (uri.getQuery() != null && !uri.getQuery().isEmpty()) {
            throw new IllegalArgumentException("Query must be empty");
        }

        final String charset = "UTF-8";
        final String algorithm = "RSA-SHA256";
        final String keyId = String.format("/%s/keys/%s",
                config.getMantaUser(), config.getMantaKeyId());
        final String keyIdEncoded = URLEncoder.encode(keyId, charset);

        StringBuilder sigText = new StringBuilder();
        sigText.append(method).append("\n")
                .append(uri.getHost()).append("\n")
                .append(uri.getPath()).append("\n")
                .append("algorithm=").append(algorithm).append("&")
                .append("expires=").append(expires).append("&")
                .append("keyId=").append(keyIdEncoded);


        StringBuilder request = new StringBuilder();
        final byte[] sigBytes = sigText.toString().getBytes();
        final byte[] signed = signer.get().sign(config.getMantaUser(), config.getMantaKeyId(), keyPair, sigBytes);
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
