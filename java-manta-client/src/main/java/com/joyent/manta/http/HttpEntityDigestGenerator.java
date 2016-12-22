package com.joyent.manta.http;

import com.joyent.manta.util.DigestNoopOutputStream;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.codec.digest.MessageDigestAlgorithms;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utility class providing methods for generating cryptographic digests for
 * HTTP entities that are repeatable. Currently only MD5 and SHA256 are
 * supported.
 */
public final class HttpEntityDigestGenerator {
    private static final boolean MD5_SUPPORTED = isDigestSupported(MessageDigestAlgorithms.MD5);
    private static final boolean SHA256_SUPPORTED = isDigestSupported(MessageDigestAlgorithms.SHA_256);

    /**
     * Private constructor because this is a utility class.
     */
    private HttpEntityDigestGenerator() {
    }

    public static boolean canGenerateDigest(final HttpEntity entity) throws IOException {
        return MD5_SUPPORTED && SHA256_SUPPORTED
                && (entity.isRepeatable() || entity.getContent().markSupported());
    }

    public static HttpEntityCalculatedDigest generate(final HttpEntity entity) throws IOException {
        final MessageDigest[] digests = new MessageDigest[] {
                DigestUtils.getMd5Digest(),
                DigestUtils.getSha256Digest()
        };

        final InputStream is = entity.getContent();

        try (DigestNoopOutputStream out = new DigestNoopOutputStream(digests)) {
            // We support repeatability with input streams that support mark()
            if (!entity.isRepeatable()) {
                is.mark(Integer.MAX_VALUE);
            }

            IOUtils.copy(is, out);

            byte[] md5 = digests[0].digest();
            byte[] sha256 = digests[1].digest();
            HttpEntityCalculatedDigest calculated = new HttpEntityCalculatedDigest(md5, sha256);

            // If we were using the mark functionality, then we reset to the original position
            if (!entity.isRepeatable()) {
                is.reset();
            }

            return calculated;
        } finally {
            if (entity.isRepeatable()) {
                IOUtils.closeQuietly(is);
            }
        }
    }

    private static boolean isDigestSupported(final String digestName) {
        try {
            MessageDigest.getInstance(digestName);
        } catch (NoSuchAlgorithmException e) {
            return false;
        }

        return true;
    }
}
