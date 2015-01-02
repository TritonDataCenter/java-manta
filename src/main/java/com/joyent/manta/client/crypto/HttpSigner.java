/**
 * Copyright (c) 2013, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client.crypto;

import com.google.api.client.http.HttpRequest;
import com.joyent.manta.exception.MantaCryptoException;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMReader;
import org.bouncycastle.openssl.PasswordFinder;
import org.bouncycastle.util.encoders.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * Joyent HTTP authorization signer. This adheres to the specs of the node-http-signature spec.
 *
 * @author Yunong Xiao
 */
public final class HttpSigner {
    private static final Logger LOG = LoggerFactory.getLogger(HttpSigner.class);
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy zzz");
    private static final String AUTHZ_HEADER = "Signature keyId=\"/%s/keys/%s\",algorithm=\"rsa-sha256\","
            + "signature=\"%s\"";
    private static final String AUTHZ_SIGNING_STRING = "date: %s";
    private static final String AUTHZ_PATTERN = "signature=\"";
    /**
     * The signing algorithm.
     */
    static final String SIGNING_ALGORITHM = "SHA256WithRSAEncryption";

    /**
     * Returns a new {@link HttpSigner} instance that can be used to sign and verify requests according to the
     * joyent-http-signature spec.
     *
     * @see <a href="http://github.com/joyent/node-http-signature/blob/master/http_signing.md">node-http-signature</a>
     * @param keyPath
     *            The path to the rsa key on disk.
     * @param fingerPrint
     *            The fingerprint of the rsa key.
     * @param login
     *            The login of the user account.
     * @return An instance of {@link HttpSigner}.
     * @throws IOException
     *             If the key is invalid.
     */
    public static HttpSigner newInstance(final String keyPath, final String fingerPrint, final String login)
            throws IOException {
        return new HttpSigner(keyPath, fingerPrint, login);
    }

    /**
     * Returns a new {@link HttpSigner} instance that can be used to sign and verify requests according to the
     * joyent-http-signature spec.
     *
     * @see <a href="http://github.com/joyent/node-http-signature/blob/master/http_signing.md">node-http-signature</a>
     * @param privateKeyContent
     *            The actual private key.
     * @param fingerPrint
     *            The fingerprint of the rsa key.
     * @param keyPassword
     *            The password to the key (optional)
     * @param login
     *            The login of the user account.
     * @return An instance of {@link HttpSigner}
     * @throws IOException
     *             If an IO exception has occured.
     */
    public static HttpSigner newInstance(final String privateKeyContent, final String fingerPrint,
                                         final char[] keyPassword, final String login) throws IOException {
        return new HttpSigner(privateKeyContent, fingerPrint, keyPassword, login);
    }

    /**
     * Keypair used to sign requests.
     */
    final KeyPair keyPair_;
    private final String login_;

    private final String fingerPrint_;

    /**
     * @param keyPath
     * @throws IOException
     */
    private HttpSigner(final String keyPath, final String fingerprint, final String login) throws IOException {
        LOG.debug(String.format("initializing HttpSigner with keypath: %s, fingerprint: %s, login: %s", keyPath,
                                fingerprint, login));
        this.fingerPrint_ = fingerprint;
        this.login_ = login;
        this.keyPair_ = this.getKeyPair(keyPath);
    }

    /**
     * @param privateKeyContent
     * @param fingerPrint
     * @param password
     * @param login
     * @throws IOException
     */
    private HttpSigner(final String privateKeyContent, final String fingerPrint, final char[] password,
                       final String login) throws IOException {
        // not logging sensitive stuff like key and password
        LOG.debug(String.format("initializing HttpSigner with private key, fingerprint: %s, password and login: %s",
                                fingerPrint, login));
        this.login_ = login;
        this.fingerPrint_ = fingerPrint;
        this.keyPair_ = this.getKeyPair(privateKeyContent, password);
    }

    /**
     * @param keyPath
     * @return
     * @throws IOException
     *             If unable to read the private key from the file
     */
    private KeyPair getKeyPair(final String keyPath) throws IOException {
        final BufferedReader br = new BufferedReader(new FileReader(keyPath));
        Security.addProvider(new BouncyCastleProvider());
        final PEMReader pemReader = new PEMReader(br);
        final KeyPair kp = (KeyPair) pemReader.readObject();
        pemReader.close();
        return kp;
    }

    /**
     * Read KeyPair from a string, optionally using password.
     *
     * @param privateKeyContent
     * @param password
     * @return
     * @throws IOException
     *             If unable to read the private key from the string
     */
    private KeyPair getKeyPair(final String privateKeyContent, final char[] password) throws IOException {
        BufferedReader reader = null;
        PEMReader pemReader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(privateKeyContent.getBytes())));
            pemReader = new PEMReader(reader, password == null ? null : new PasswordFinder() {
                @Override
                public char[] getPassword() {
                    return password;
                }
            });
            return (KeyPair) pemReader.readObject();

        } finally {
            if (reader != null) {
                reader.close();
            }
            if (pemReader != null) {
                pemReader.close();
            }
        }

    }

    /**
     * Sign an {@link HttpRequest}.
     *
     * @param request
     *            The {@link HttpRequest} to sign.
     * @throws MantaCryptoException
     *             If unable to sign the request.
     */
    public void signRequest(final HttpRequest request) throws MantaCryptoException {
        LOG.debug("signing request: " + request.getHeaders());
        String date = request.getHeaders().getDate();
        if (this.keyPair_ == null) {
            throw new MantaCryptoException("keys not loaded");
        }
        if (date == null) {
            final Date now = Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTime();
            date = DATE_FORMAT.format(now);
            LOG.debug("setting date header: " + date);
            request.getHeaders().setDate(date);
        }
        try {
            final Signature sig = Signature.getInstance(SIGNING_ALGORITHM);
            sig.initSign(this.keyPair_.getPrivate());
            final String signingString = String.format(AUTHZ_SIGNING_STRING, date);
            sig.update(signingString.getBytes("UTF-8"));
            final byte[] signedDate = sig.sign();
            final byte[] encodedSignedDate = Base64.encode(signedDate);
            final String authzHeader = String.format(AUTHZ_HEADER, this.login_, this.fingerPrint_,
                                                     new String(encodedSignedDate));
            request.getHeaders().setAuthorization(authzHeader);
        } catch (final NoSuchAlgorithmException e) {
            throw new MantaCryptoException("invalid algorithm", e);
        } catch (final InvalidKeyException e) {
            throw new MantaCryptoException("invalid key", e);
        } catch (final SignatureException e) {
            throw new MantaCryptoException("invalid signature", e);
        } catch (final UnsupportedEncodingException e) {
            throw new MantaCryptoException("invalid encoding", e);
        }
    }

    /**
     * Verify a signed {@link HttpRequest}.
     *
     * @param request
     *            The signed {@link HttpRequest}.
     * @return True if the request is valid, false if not.
     * @throws MantaCryptoException
     *             If unable to verify the request.
     */
    public boolean verifyRequest(final HttpRequest request) throws MantaCryptoException {
        LOG.debug("verifying request: " + request.getHeaders());
        String date = request.getHeaders().getDate();
        if (date == null) {
            throw new MantaCryptoException("no date header in request");
        }

        date = String.format(AUTHZ_SIGNING_STRING, date);

        try {
            final Signature verify = Signature.getInstance(SIGNING_ALGORITHM);
            verify.initVerify(this.keyPair_.getPublic());
            final String authzHeader = request.getHeaders().getAuthorization();
            final int startIndex = authzHeader.indexOf(AUTHZ_PATTERN);
            if (startIndex == -1) {
                throw new MantaCryptoException("invalid authorization header " + authzHeader);
            }
            final String encodedSignedDate = authzHeader.substring(startIndex + AUTHZ_PATTERN.length(),
                                                                   authzHeader.length() - 1);
            final byte[] signedDate = Base64.decode(encodedSignedDate.getBytes("UTF-8"));
            verify.update(date.getBytes("UTF-8"));
            return verify.verify(signedDate);
        } catch (final NoSuchAlgorithmException e) {
            throw new MantaCryptoException("invalid algorithm", e);
        } catch (final InvalidKeyException e) {
            throw new MantaCryptoException("invalid key", e);
        } catch (final SignatureException e) {
            throw new MantaCryptoException("invalid signature", e);
        } catch (final UnsupportedEncodingException e) {
            throw new MantaCryptoException("invalid encoding", e);
        }
    }
}
