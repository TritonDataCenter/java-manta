/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.config;

import com.joyent.manta.client.crypto.AesGcmCipherDetails;
import com.joyent.manta.exception.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Test
public class ConfigContextTest {
    private final byte[] keyBytes;

    public static final String MANTA_AUTH_PRIVATE_KEY = "-----BEGIN RSA PRIVATE KEY-----\n" +
            "MIIEpQIBAAKCAQEA1lPONrT34W2VPlltA76E2JUX/8+Et7PiMiRNWAyrATLG7aRA\n" +
            "8iZ5A8o/aQMyexp+xgXoJIh18LmJ1iV8zqnr4TPXD2iPO92fyHWPu6P+qn0uw2Hu\n" +
            "ZZ0IvHHYED+fqxm7jz2ZjnfZl5Bz73ctjRF+77rPgOhhfv4KAc1d9CDsC+lHTqbp\n" +
            "ngufCYI4UWrnYoQ2JVXvEL9D5dMlHg0078qfh2cPg5xMOiOYobZeWqflV1Ue5I1Y\n" +
            "owNqiFzIDmBK0TKhnv+qQVNfMnNLJBYlYyGd0DUOJs8os5yivtuQXOhLZ0zLiTqK\n" +
            "JVjNJLzlcciqUf97Btm2enEHJ/khMFhrmoTQFQIDAQABAoIBAQCdc//grN4WHD0y\n" +
            "CtxNjd9mhVGWOsvTcTFRiN3RO609OiJuXubffmgU4rXm3dRuH67Wp2w9uop6iLO8\n" +
            "QNoJsUd6sGzkAvqHDm/eAo/PV9E1SrXaD83llJHgbvo+JZ+VQVhLCQQQZ/fQouyp\n" +
            "FbK/GgVY9LKQjydg9hw/6rGFMdJ3hFZVFqYFUhNpQKpczi6/lI/UIGcBhF3+8s/0\n" +
            "KMrz2PcCQFixlUFtBYXQHarOctxJDX7indchX08buwPqSv4YBBDLHUZkkMWomI/P\n" +
            "NjRDRyqnxvI03lHVfdbDzoPMxklJlHF68fkmp8NFLegnCBM8K0ae65Vk61b3oF9X\n" +
            "3eD6JtAZAoGBAPo/oBaJlA0GbQoJmULj6YqcQ2JKbUJtu7LP//8Gss47po4uqh6n\n" +
            "9vneKEpYYxuH5MXNsqtinmSQQMkE4UXoJSxJvnXNVAMQa3kUd0UgZSHjqWWgauDj\n" +
            "BjLQRpy9evef7VzTYx0xqEfAprsXxAoy0KXYN8gwgMC6MQgfZuFBgtxLAoGBANtA\n" +
            "1SVN/4wqrz4C8rpx7oZarHcMmGLiFF5OpKXlq1JY+U8IJ+WxMId3TI4h/h6OQGth\n" +
            "NJzQqFCS9H3a5EmqoNXHsLVXiKtG40+OzphSf9Y/NU7FtKanFWjfZl1ihhran1Fc\n" +
            "42jzN34EMM7Wm8p6HUK5qiDSCF+Ck0Lupud+WIkfAoGAXREOg3M0+UcbhDEfq23B\n" +
            "bAhDUymkyqCuvoh2hyzBkMtEXPpj0DTdN/3z8/o9GX8HiLzAJtbtWy7+uQO0l+AG\n" +
            "+xqN15e+F8mifowq8y1iDyFw3Ve0h+BGbN1idWZOdgsnJm+DG9dc4xp1p3zmLnjJ\n" +
            "efQYgr3vFD3qgD/Vbg6EEVMCgYEAnNfaIh+T6Y83YWL2hI2wFgiTS26FLGeSLoyP\n" +
            "l+WeEwB3CCRLdjK1BpM+/oYupWkZiDc3Td6uKUWXBNkrac9X0tZRAMinie7h+S2t\n" +
            "eKW7sWXyGnGv82+fDzCQp8ktKdSvF6MdQxyJ2+nfiHdZZxTIDc2HeIcHWlusQLs8\n" +
            "RmnJp/0CgYEA8AUV7K2KNRcwfuB1UjqhvlaqgiGixrItacGgnMQJ2cRSRSq2fZTm\n" +
            "eXxT9ugZ/9J9D4JTYZgdABnKvDjgbJMH9w8Nxr+kn/XZKNDzc1z0iJYwvyBOc1+e\n" +
            "prHvy4y+bCc0kLjCNQW4+/pVTWe1w8Mp63Vhdn+fO+wUGT3DTJGIXkU=\n" +
            "-----END RSA PRIVATE KEY-----";

    {
        keyBytes = "FFFFFFFBD96783C6C91E2222".getBytes(StandardCharsets.US_ASCII);
    }

    @Test(expectedExceptions = ConfigurationException.class)
    public void validationWillFailWithNothingSet() {
        StandardConfigContext config = new StandardConfigContext();
        ConfigContext.validate(config);
    }

    public void canValidateContextWithClientEncryptionDisabled() throws IOException {
        File mantaAuthPrivateKey = File.createTempFile("manta-key", "");
        FileUtils.forceDeleteOnExit(mantaAuthPrivateKey);
        FileUtils.write(mantaAuthPrivateKey, MANTA_AUTH_PRIVATE_KEY, StandardCharsets.US_ASCII);

        StandardConfigContext config = new StandardConfigContext();
        config.setMantaURL(DefaultsConfigContext.DEFAULT_MANTA_URL);
        config.setMantaUser("username");
        config.setMantaKeyId("ac:95:92:ff:88:f7:3d:cd:ba:23:7b:54:44:21:60:02");
        config.setMantaKeyPath(mantaAuthPrivateKey.getAbsolutePath());
        config.setClientEncryptionEnabled(false);
    }

    public void canValidateContextWithKeyPaths() throws IOException {
        File mantaAuthPrivateKey = File.createTempFile("manta-key", "");
        FileUtils.forceDeleteOnExit(mantaAuthPrivateKey);
        FileUtils.write(mantaAuthPrivateKey, MANTA_AUTH_PRIVATE_KEY, StandardCharsets.US_ASCII);

        File encryptionPrivateKey = File.createTempFile("encryption-key", "");
        FileUtils.forceDeleteOnExit(encryptionPrivateKey);
        FileUtils.writeByteArrayToFile(encryptionPrivateKey, keyBytes);

        StandardConfigContext config = new StandardConfigContext();
        config.setMantaURL(DefaultsConfigContext.DEFAULT_MANTA_URL);
        config.setMantaUser("username");
        config.setMantaKeyId("ac:95:92:ff:88:f7:3d:cd:ba:23:7b:54:44:21:60:02");
        config.setMantaKeyPath(mantaAuthPrivateKey.getAbsolutePath());
        config.setClientEncryptionEnabled(true);
        config.setEncryptionKeyId("test-key-1");
        config.setEncryptionAuthenticationMode(EncryptionAuthenticationMode.DEFAULT_MODE);
        config.setPermitUnencryptedDownloads(false);
        config.setEncryptionPrivateKeyPath(encryptionPrivateKey.getAbsolutePath());
        config.setEncryptionAlgorithm(AesGcmCipherDetails.INSTANCE_128_BIT.getCipherId());
        ConfigContext.validate(config);
    }

    public void canValidateContextWithKeyValues() throws IOException {
        StandardConfigContext config = new StandardConfigContext();
        config.setMantaURL(DefaultsConfigContext.DEFAULT_MANTA_URL);
        config.setMantaUser("username");
        config.setMantaKeyId("ac:95:92:ff:88:f7:3d:cd:ba:23:7b:54:44:21:60:02");
        config.setPrivateKeyContent(MANTA_AUTH_PRIVATE_KEY);
        config.setClientEncryptionEnabled(true);
        config.setEncryptionKeyId("test-key-1");
        config.setEncryptionAuthenticationMode(EncryptionAuthenticationMode.DEFAULT_MODE);
        config.setPermitUnencryptedDownloads(false);
        config.setEncryptionPrivateKeyBytes(keyBytes);
        config.setEncryptionAlgorithm(AesGcmCipherDetails.INSTANCE_128_BIT.getCipherId());
        ConfigContext.validate(config);
    }
}
