/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.cli;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.config.ChainedConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.EnvVarConfigContext;
import com.joyent.manta.config.MapConfigContext;
import com.joyent.manta.util.MantaVersion;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.stream.Stream;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * Class providing a CLI interface to the Java Manta SDK.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public final class MantaCLI {
    /**
     * System dependent line-break.
     */
    private static final String BR = System.lineSeparator();

    /**
     * Indent spaces to sub elements.
     */
    private static final String INDENT = "  ";

    /**
     * Private constructor for class only containing static methods.
     */
    private MantaCLI() {
    }

    /**
     * Entrance to the CLI.
     * @param argv parameters passed.
     */
    public static void main(final String[] argv) {
        if (ArrayUtils.isEmpty(argv) || StringUtils.isBlank(argv[0])) {
            System.out.println(help());
            return;
        }

        final String cmd = argv[0];

        try {
            switch (cmd) {
                case "connect-test":
                    System.out.println(connectTest());
                    break;
                case "dump-config":
                    System.out.println(dumpConfig());
                    break;
                case "generate-key":
                    if (argv.length < 4) {
                        System.err.println(help());
                        System.err.println();
                        System.err.println("generate-key requires three parameters: cipher, bits and path");
                        break;
                    }

                    System.out.println(generateKey(argv[1].trim(), Integer.valueOf(argv[2].trim()),
                            Paths.get(argv[3].trim())));
                    break;
                case "ls":
                    if (argv.length < 2) {
                        System.err.println(help());
                        System.err.println();
                        System.err.println("ls requires one parameter: dir");
                        break;
                    }
                    System.out.println(listDir(argv[1].trim()));
                    break;
                case "get-file":
                    if (argv.length < 2) {
                        System.err.println(help());
                        System.err.println();
                        System.err.println("get-file requires one parameter: filePath");
                        break;
                    }

                    System.out.println(getFile(argv[1].trim()));
                    break;
                case "put-file":
                    if (argv.length < 3) {
                        System.err.println(help());
                        System.err.println();
                        System.err.println("put-file requires two parameters: localFilePath and Manta filePath");
                        break;
                    }

                    System.out.println(putFile(argv[1].trim(), argv[2].trim()));
                    break;
                case "validate-key":
                    if (argv.length < 3) {
                        System.err.println(help());
                        System.err.println();
                        System.err.println("validate-key requires two parameters: cipher and path");
                        break;
                    }

                    System.out.println(validateKey(argv[1].trim(),
                            Paths.get(argv[3].trim())));
                    break;
                case "help":
                default:
                    System.out.println(help());
            }
        } catch (Exception e) {
            System.err.println(e.getLocalizedMessage());
            System.err.println(ExceptionUtils.getStackTrace(e));
            System.exit(1);
        }
    }

    /**
     * Displays help for the CLI.
     * @return string containing help text
     */
    public static String help() {
        StringBuilder b = new StringBuilder();

        String jar = String.format("java-manta-client-%s.jar", MantaVersion.VERSION);

        b.append("Java Manta SDK").append(BR)
         .append(MantaVersion.VERSION).append(" ").append(MantaVersion.DATE).append(BR)
         .append("========================================").append(BR)
         .append(BR)
         .append("java -jar ").append(jar).append(" <command>").append(BR)
         .append(BR)
         .append("Commands:").append(BR)
         .append(INDENT).append("connect-test                        ")
                        .append("Attempts to connect to Manta using system properties and environment variables for configuration.").append(BR)
         .append(INDENT).append("dump-config                         ")
                        .append("Dumps the configuration that was loaded using defaults, system properties and environment variables").append(BR)
         .append(INDENT).append("generate-key <cipher> <bits> <path> ")
                        .append("Generates a client-side encryption key and saves it to the specified location").append(BR)
         .append(INDENT).append("validate-key <cipher> <path> ")
         .append("Validates that the client-side encryption key is loadable by the Manta SDK").append(BR)
         .append(INDENT).append("help                                ")
                        .append("Displays this message");

        return b.toString();
    }

    /**
     * Performs a test connection to Manta.
     * @return String containing the output of the operation
     * @throws IOException thrown when we are unable to connect to Manta
     */
    protected static String connectTest() throws IOException {
        final StringBuilder b = new StringBuilder();

        b.append("Creating connection configuration").append(BR);
        ConfigContext config = buildConfig();
        b.append(INDENT).append(ConfigContext.toString(config)).append(BR);

        b.append("Creating new connection object").append(BR);
        try (MantaClient client = new MantaClient(config)) {
            b.append(INDENT).append(client).append(BR);

            String homeDirPath = config.getMantaHomeDirectory();
            b.append("Attempting HEAD request to: ").append(homeDirPath).append(BR);

            MantaObjectResponse response = client.head(homeDirPath);
            b.append(INDENT).append(response).append(BR);
            b.append("Request was successful");
        }

        return b.toString();
    }

    /**
     * Dumps the configuration used for configuring a Manta client.
     * @return String containing the contents of the parsed configuration
     */
    protected static String dumpConfig() {
        StringBuilder b = new StringBuilder();

        ConfigContext config = buildConfig();
        b.append(ConfigContext.toString(config));

        return b.toString();
    }

    /**
     * Generates a client-side encryption key with the specified cipher and bits
     * at the specified path.
     *
     * @param cipher cipher to generate key for
     * @param bits number of bits of the key
     * @param path path to write the key to
     * @return String containing the output of the operation
     */
    protected static String generateKey(final String cipher, final int bits,
                                        final Path path) {
        StringBuilder b = new StringBuilder();

        try {
            b.append("Generating key").append(BR);
            SecretKey key = SecretKeyUtils.generate(cipher, bits);

            b.append(String.format("Writing [%s-%d] key to [%s]", cipher, bits, path));
            b.append(BR);
            SecretKeyUtils.writeKeyToPath(key, path);
        } catch (NoSuchAlgorithmException e) {
            System.err.printf("The running JVM [%s/%s] doesn't support the "
                + "supplied cipher name [%s]", System.getProperty("java.version"),
                    System.getProperty("java.vendor"), cipher);
            System.err.println();
            return "";
        } catch (IOException e) {
            String msg = String.format("Unable to write key to path [%s]",
                    path);
            throw new UncheckedIOException(msg, e);
        }

        return b.toString();
    }


    /**
     * ls.
     *
     * @param dirPath dir to ls
     * @return String containing the output of the operation
     * @throws IOException thrown when we are unable to connect to Manta
     */
    protected static String listDir(final String dirPath) throws IOException {
        final StringBuilder b = new StringBuilder();
        ConfigContext config = buildConfig();
        try (MantaClient client = new MantaClient(config)) {
        final Stream<MantaObject> objs = client.listObjects(dirPath);
        objs.forEach(obj -> {
                b.append(INDENT).append(obj.getPath()).append(BR);
            });
        }

        return b.toString();
    }

    /**
     * Performs a download of file in Manta.
     * @param filePath in Manta to download
     * @return String containing the output of the operation
     * @throws IOException thrown when we are unable to connect to Manta
     */
    protected static String getFile(final String filePath) throws IOException {
        final StringBuilder b = new StringBuilder();

        b.append("Creating connection configuration").append(BR);
        ConfigContext config = buildConfig();
        b.append(INDENT).append(ConfigContext.toString(config)).append(BR);

        b.append("Creating new connection object").append(BR);
        try (MantaClient client = new MantaClient(config)) {
            b.append(INDENT).append(client).append(BR);

            b.append("Attempting GET request to: ").append(filePath).append(BR);
            b.append("\nMetadata values: \n").append(client.get(filePath).getMetadata().toString());
            b.append("\n\nPayload: \n");

            String response = client.getAsString(filePath);
            b.append(INDENT).append(response).append(BR);
            b.append("Request was successful");
        }

        return b.toString();
    }

    /**
     * Performs a put of a local file to Manta.
     * @param filePath of file to upload/put
     * @param mantaPath in Manta to upload to
     * @return String containing the output of the operation
     * @throws IOException thrown when we are unable to connect or upload to Manta
     */
    protected static String putFile(final String filePath, final String mantaPath) throws IOException {
        final StringBuilder b = new StringBuilder();

        b.append("Creating connection configuration").append(BR);
        ConfigContext config = buildConfig();
        b.append(INDENT).append(ConfigContext.toString(config)).append(BR);

        b.append("Creating new connection object").append(BR);
        try (MantaClient client = new MantaClient(config)) {
            b.append(INDENT).append(client).append(BR);

            b.append("Attempting PUT request to: ").append(filePath).append(BR);
            File file = new File(filePath);
            MantaObjectResponse response = client.put(mantaPath, file);
            b.append(response.toString());
            b.append("Request was successful");
        }

        return b.toString();
    }

    /**
     * Validates that the supplied key is supported by the SDK's client-side
     * encryption functionality.
     *
     * @param cipher cipher to validate the key against
     * @param path path to read the key from
     * @return String containing the output of the operation
     */
    protected static String validateKey(final String cipher, final Path path) {
        StringBuilder b = new StringBuilder();

        try {
            b.append(String.format("Loading key from path [%s]", path)).append(BR);
            SecretKeySpec key = SecretKeyUtils.loadKeyFromPath(path, cipher);

            if (key.getAlgorithm().equals(cipher)) {
                b.append("Cipher of key is [")
                 .append(cipher)
                 .append("] as expected")
                 .append(BR);
            } else {
                b.append("Cipher of key is [")
                 .append(key.getAlgorithm())
                 .append("] - it doesn't match the expected cipher of [")
                 .append(cipher)
                 .append("]").append(BR);
            }

            b.append("Key format is [")
             .append(key.getFormat())
             .append("]").append(BR);
        } catch (NoSuchAlgorithmException e) {
            System.err.printf("The running JVM [%s/%s] doesn't support the "
                            + "supplied cipher name [%s]", System.getProperty("java.version"),
                    System.getProperty("java.vendor"), cipher);
            System.err.println();
            return "";
        } catch (IOException e) {
            String msg = String.format("Unable to read key from path [%s]",
                    path);
            throw new UncheckedIOException(msg, e);
        }

        return b.toString();
    }

    /**
     * Builds a new {@link ConfigContext} instance based on defaults and
     * environment variables.
     *
     * @return chained configuration context object
     */
    private static ConfigContext buildConfig() {
        return new ChainedConfigContext(
                new DefaultsConfigContext(),
                new EnvVarConfigContext(),
                new MapConfigContext(System.getProperties()));
    }
}
