package com.joyent.manta.http;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.Validate;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;

import java.util.Base64;

public class HttpEntityCalculatedDigest {
    private final byte[] md5;
    private final byte[] sha256;

    public HttpEntityCalculatedDigest(final byte[] md5, final byte[] sha256) {
        Validate.notNull(md5, "MD5 digest must not be null");
        Validate.notNull(sha256, "SHA256 digest must not be null");

        this.md5 = md5;
        this.sha256 = sha256;
    }

    public byte[] getMd5() {
        return md5;
    }

    public String getMd5AsHexString() {
        return Hex.encodeHexString(this.md5);
    }

    public String getMd5AsBase64() {
        return Base64.getEncoder().encodeToString(this.md5);
    }

    public byte[] getSha256() {
        return sha256;
    }

    public String getSha256AsHexString() {
        return Hex.encodeHexString(this.sha256);
    }

    public String getSha256AsBase64() {
        return Base64.getEncoder().encodeToString(this.sha256);
    }

    public void addAsHeaders(final MantaHttpHeaders headers) {
        Validate.notNull(headers, "Headers must not be null");
        headers.put(HttpHeaders.CONTENT_MD5, getMd5AsBase64());
        headers.put(MantaHttpHeaders.CONTENT_SHA256, getSha256AsBase64());
    }
}
