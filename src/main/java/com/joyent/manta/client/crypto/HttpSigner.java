package com.joyent.manta.client.crypto;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMReader;
import org.bouncycastle.util.encoders.Base64;

import com.google.api.client.http.HttpRequest;
import com.joyent.manta.exception.MantaCryptoException;

/**
 * 
 * @author yunong
 * 
 */
public class HttpSigner {
        private static final Log LOG = LogFactory.getLog(HttpSigner.class);
        private static final DateFormat DATE_FORMAT = new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy");
        private static final String AUTHZ_HEADER = "Signature keyId=\"/%s/keys/%s\",algorithm=\"rsa-sha256\" %s";
        private static final String AUTHZ_PATTERN = "algorithm=\"rsa-sha256\" ";
        static final String SIGNING_ALGORITHM = "SHA256WithRSAEncryption";

        /**
         * 
         */
        final KeyPair keyPair_;
        private final String login_;
        private final String fingerPrint_;

        /**
         * @param keyPath
         * @throws IOException
         */
        private HttpSigner(String keyPath, String fingerprint, String login) throws IOException {
                LOG.debug(String.format("initializing HttpSigner with keypath: %s, fingerprint: %s, login: %s",
                                        keyPath, fingerprint, login));
                fingerPrint_ = fingerprint;
                login_ = login;
                keyPair_ = getKeyPair(keyPath);
        }

        /**
         * @param keyPath
         * @return
         * @throws IOException
         */
        public static final HttpSigner newInstance(String keyPath, String fingerPrint, String login) throws IOException {
                return new HttpSigner(keyPath, fingerPrint, login);
        }

        /**
         * @param request
         * @throws MantaCryptoException
         */
        public final void signRequest(HttpRequest request) throws MantaCryptoException {
                LOG.debug("signing request: " + request.getHeaders());
                String date = request.getHeaders().getDate();
                if (date == null) {
                        Date now = Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTime();
                        date = DATE_FORMAT.format(now);
                        request.getHeaders().setDate(date);
                }
                Signature sig;
                byte[] signedDate;
                try {
                        sig = Signature.getInstance(SIGNING_ALGORITHM);
                        sig.initSign(keyPair_.getPrivate());
                        sig.update(date.getBytes("UTF-8"));
                        signedDate = sig.sign();
                        byte[] encodedSignedDate = Base64.encode(signedDate);
                        String authzHeader = String.format(AUTHZ_HEADER, login_, fingerPrint_, new String(
                                encodedSignedDate));
                        LOG.debug("authorization header is: " + authzHeader);
                        request.getHeaders().setAuthorization(authzHeader);
                } catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException
                         | UnsupportedEncodingException e) {
                        throw new MantaCryptoException("unable to sign request", e);
                }
        }

        /**
         * 
         * @param request
         * @return
         * @throws MantaCryptoException
         */
        public final boolean verifyRequest(HttpRequest request) throws MantaCryptoException {
                LOG.debug("verifying request: " + request.getHeaders());
                String date = request.getHeaders().getDate();
                if (date == null) {
                        throw new MantaCryptoException("no date header in request");
                }

                try {
                        Signature verify = Signature.getInstance(SIGNING_ALGORITHM);
                        verify.initVerify(keyPair_.getPublic());
                        String authzHeader = request.getHeaders().getAuthorization();
                        int startIndex = authzHeader.indexOf(AUTHZ_PATTERN);
                        if (startIndex == -1) {
                                throw new MantaCryptoException("invalid authorization header " + authzHeader);
                        }
                        String encodedSignedDate = authzHeader.substring(startIndex + AUTHZ_PATTERN.length());
                        LOG.debug("got encoded signed date: " + encodedSignedDate);
                        byte[] signedDate = Base64.decode(encodedSignedDate.getBytes("UTF-8"));
                        verify.update(date.getBytes("UTF-8"));
                        return verify.verify(signedDate);
                } catch (NoSuchAlgorithmException | InvalidKeyException | UnsupportedEncodingException
                         | SignatureException e) {
                        throw new MantaCryptoException("unable to verify request", e);
                }
        }

        /**
         * @param keyPath
         * @return
         * @throws IOException
         */
        private final KeyPair getKeyPair(String keyPath) throws IOException {
                BufferedReader br = new BufferedReader(new FileReader(keyPath));
                Security.addProvider(new BouncyCastleProvider());
                PEMReader pemReader = new PEMReader(br);
                KeyPair kp = (KeyPair) pemReader.readObject();
                pemReader.close();
                return kp;
        }
}
