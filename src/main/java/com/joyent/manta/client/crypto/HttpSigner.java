/**
 * Copyright (c) 2013, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client.crypto;

import com.google.api.client.http.HttpRequest;
import com.joyent.manta.exception.MantaCryptoException;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.bouncycastle.util.encoders.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.security.Signature;
import java.security.SignatureException;
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

    /**
     * The static logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(HttpSigner.class);

    /**
     * The format for the http date header.
     */
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy zzz");

    /**
     * The template for the Authorization header.
     */
    private static final String AUTHZ_HEADER =
            "Signature keyId=\"/%s/keys/%s\",algorithm=\"rsa-sha256\",signature=\"%s\"";

    /**
     * The template for the authorization signing signing string.
     */
    private static final String AUTHZ_SIGNING_STRING = "date: %s";

    /**
     * The prefix for the signature component of the authorization header.
     */
    private static final String AUTHZ_PATTERN = "signature=\"";

    /**
     * The signing algorithm.
     */
    static final String SIGNING_ALGORITHM = "SHA256WithRSAEncryption";

    /**
     * The key format converter to use when reading key pairs.
     */
    private final JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");


    /**
     * Returns a new {@link com.joyent.manta.client.crypto.HttpSigner} instance that can be used to sign and verify
     * requests according to the joyent-http-signature spec.
     *
     * @see <a href="http://github.com/joyent/node-http-signature/blob/master/http_signing.md">node-http-signature</a>
     * @param keyPath The path to the rsa key on disk.
     * @param fingerPrint The fingerprint of the rsa key.
     * @param login The login of the user account.
     * @return An instance of {@link com.joyent.manta.client.crypto.HttpSigner}.
     * @throws IOException If the key is invalid.
     */
    public static HttpSigner newInstance(final String keyPath, final String fingerPrint, final String login)
            throws IOException {
        return new HttpSigner(keyPath, fingerPrint, login);
    }


    /**
     * Returns a new {@link com.joyent.manta.client.crypto.HttpSigner} instance that can be used to sign and verify
     * requests according to the joyent-http-signature spec.
     *
     * @see <a href="http://github.com/joyent/node-http-signature/blob/master/http_signing.md">node-http-signature</a>
     * @param privateKeyContent The actual private key.
     * @param fingerPrint The fingerprint of the rsa key.
     * @param keyPassword The password to the key (optional)
     * @param login The login of the user account.
     * @return An instance of {@link com.joyent.manta.client.crypto.HttpSigner}
     * @throws IOException
     *             If an IO exception has occured.
     */
    public static HttpSigner newInstance(final String privateKeyContent,
                                         final String fingerPrint,
                                         final char[] keyPassword,
                                         final String login)
            throws IOException {
        return new HttpSigner(privateKeyContent, fingerPrint, keyPassword, login);
    }


    /**
     * Keypair used to sign requests.
     */
    private final KeyPair keyPair;


    /**
     * The account name associated with Manta account.
     */
    private final String login;

    /**
     * The RSA key fingerprint.
     */
    private final String fingerprint;


    /**
     * Creates a new instance of the HttpSigner.
     *
     * @param keyPath The path to the rsa key on disk
     * @param fingerprint rsa key fingerprint
     * @param login account name associated with Manta account
     * @throws IOException thrown on network error or on filesystem error
     */
    private HttpSigner(final String keyPath, final String fingerprint, final String login) throws IOException {
        LOG.debug(
                String.format(
                        "initializing HttpSigner with keypath: %s, fingerprint: %s, login: %s",
                        keyPath,
                        fingerprint,
                        login
                )
        );
        this.fingerprint = fingerprint;
        this.login = login;
        this.keyPair = this.getKeyPair(keyPath);
    }


    /**
     * Creates a new instance of the HttpSigner.
     *
     * @param privateKeyContent private key content as a string
     * @param fingerprint rsa key fingerprint
     * @param password password associated with key
     * @param login account name associated with Manta account
     * @throws IOException thrown on network error or on filesystem error
     */
    private HttpSigner(final String privateKeyContent, final String fingerprint, final char[] password,
                       final String login) throws IOException {
        // not logging sensitive stuff like key and password
        LOG.debug(
                String.format(
                        "initializing HttpSigner with private key, fingerprint: %s, password and login: %s",
                        fingerprint,
                        login
                )
        );
        this.login = login;
        this.fingerprint = fingerprint;
        this.keyPair = this.getKeyPair(privateKeyContent, password);
    }


    /**
     * Read KeyPair located at the specified path.
     *
     * @param keyPath The path to the rsa key on disk
     * @return public-private keypair object
     * @throws IOException If unable to read the private key from the file
     */
    private KeyPair getKeyPair(final String keyPath) throws IOException {
        if (keyPath == null) {
            throw new FileNotFoundException("No key file path specified");
        }

        File keyFile = new File(keyPath);

        if (!keyFile.exists()) {
            throw new FileNotFoundException(
                    String.format("No key file available at path: %s", keyFile));
        }

        if (!keyFile.canRead()) {
            throw new IOException(
                    String.format("Can't read key file from path: %s", keyFile));
        }

        final BufferedReader br = new BufferedReader(new FileReader(keyFile));
        Security.addProvider(new BouncyCastleProvider());

        try (final PEMParser pemParser = new PEMParser(br)) {
            final Object object = pemParser.readObject();

            return converter.getKeyPair((PEMKeyPair) object);
        }
    }


    /**
     * Read KeyPair from a string, optionally using password.
     *
     * @param privateKeyContent private key content as a string
     * @param password password associated with key
     * @return public-private keypair object
     * @throws IOException If unable to read the private key from the string
     */
    private KeyPair getKeyPair(final String privateKeyContent, final char[] password) throws IOException {
        byte[] pKeyBytes = privateKeyContent.getBytes();

        try (InputStream byteArrayStream = new ByteArrayInputStream(pKeyBytes);
             Reader inputStreamReader = new InputStreamReader(byteArrayStream);
             BufferedReader reader = new BufferedReader(inputStreamReader);
             PEMParser pemParser = new PEMParser(reader)) {

            PEMDecryptorProvider decProv = new JcePEMDecryptorProviderBuilder().build(password);

            Object object = pemParser.readObject();

            final KeyPair kp;
            if (object instanceof PEMEncryptedKeyPair) {
                kp = converter.getKeyPair(((PEMEncryptedKeyPair) object).decryptKeyPair(decProv));
            } else {
                kp = converter.getKeyPair((PEMKeyPair) object);
            }

            return kp;

        }
    }


    /**
     * Sign an {@link com.google.api.client.http.HttpRequest}.
     *
     * @param request The {@link com.google.api.client.http.HttpRequest} to sign.
     * @throws MantaCryptoException If unable to sign the request.
     */
    public void signRequest(final HttpRequest request) throws MantaCryptoException {
        LOG.debug("signing request: " + request.getHeaders());
        String date = request.getHeaders().getDate();
        if (this.keyPair == null) {
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
            sig.initSign(this.keyPair.getPrivate());
            final String signingString = String.format(AUTHZ_SIGNING_STRING, date);
            sig.update(signingString.getBytes("UTF-8"));
            final byte[] signedDate = sig.sign();
            final byte[] encodedSignedDate = Base64.encode(signedDate);
            final String authzHeader = String.format(AUTHZ_HEADER, this.login, this.fingerprint,
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
     * Verify a signed {@link com.google.api.client.http.HttpRequest}.
     *
     * @param request The signed {@link com.google.api.client.http.HttpRequest}.
     * @return True if the request is valid, false if not.
     * @throws MantaCryptoException If unable to verify the request.
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
            verify.initVerify(this.keyPair.getPublic());
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
