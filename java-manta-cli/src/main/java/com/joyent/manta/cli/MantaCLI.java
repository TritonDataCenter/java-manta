/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.cli;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.config.ChainedConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.EnvVarConfigContext;
import com.joyent.manta.config.MapConfigContext;
import com.joyent.manta.util.MantaUtils;
import com.joyent.manta.util.MantaVersion;
import org.apache.commons.io.IOUtils;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.stream.Stream;

@CommandLine.Command(name = "java-manta-cli", sortOptions = false,
        header = {
             "@|cyan                 .     .             |@",
             "@|cyan                 |_.-._|             |@",
             "@|cyan               ./       \\.          |@",
             "@|cyan          _.-'`           `'-._      |@",
             "@|cyan       .-'        Java         '-.   |@",
             "@|cyan     ,'_.._       Manta       _.._', |@",
             "@|cyan     '`    `'-.           .-'`    `' |@",
             "@|cyan               '.       .'           |@",
             "@|cyan                 \\_/|\\_/           |@",
             "@|cyan                    |                |@",
             "@|cyan                    |                |@",
             "@|cyan                    |                |@",
             ""},
        description = {
                "",
                "Basic manta cli commands using the java-client. Not a stable interface.", },
        optionListHeading = "@|bold %nOptions|@:%n",
         subcommands = {
             MantaCLI.ConnectTest.class,
             MantaCLI.DumpConfig.class,
             MantaCLI.GenerateKey.class,
             MantaCLI.ListDir.class,
             MantaCLI.GetFile.class,
             MantaCLI.PutFile.class,
             MantaCLI.ObjectInfo.class,
             MantaCLI.ValidateKey.class
         })
// Documented through CLI annotations
@SuppressWarnings({"checkstyle:javadocmethod", "checkstyle:javadoctype", "checkstyle:javadocvariable"})
public final class MantaCLI {
    @SuppressWarnings("unused")
    @CommandLine.Option(names = {"-v", "--version"}, help = true)
    private boolean isVersionRequested;

    @SuppressWarnings("unused")
    @CommandLine.Option(names = {"-h", "--help"}, help = true)
    private boolean isHelpRequested;

    private MantaCLI() { }

    public static void main(final String[] args) {
        final CommandLine application = new CommandLine(new MantaCLI());
        application.registerConverter(Path.class, Paths::get);

        List<CommandLine> parsedCommands;
        try {
            parsedCommands = application.parse(args);
        } catch (CommandLine.ParameterException ex) {
            System.err.println(ex.getMessage());
            CommandLine.usage(new MantaCLI(), System.err);
            return;
        }
        MantaCLI cli = parsedCommands.get(0).getCommand();
        if (cli.isHelpRequested) {
            application.usage(System.out);
            return;
        }
        if (cli.isVersionRequested) {
            System.out.println("java-manta-client: " +  MantaVersion.VERSION);
            return;
        }
        if (parsedCommands.size() == 1) {
            // no subcmd given
            application.usage(System.err);
            return;
        }
        CommandLine deepest = parsedCommands.get(parsedCommands.size() - 1);

        MantaSubCommand subcommand = deepest.getCommand();
        if (subcommand.isHelpRequested) {
            CommandLine.usage(deepest.getCommand(), System.err);
            return;
        }
        if (subcommand.logLevel != null) {
            Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
            root.setLevel(Level.valueOf(subcommand.logLevel.toString()));
        }
        subcommand.run();

    }


    @CommandLine.Command(sortOptions = false,
                         headerHeading = "@|bold,underline Usage:|@%n%n",
                         synopsisHeading = "%n",
                         descriptionHeading = "%n@|bold,underline Description:|@%n%n",
                         parameterListHeading = "%n@|bold,underline Parameters:|@%n",
                         optionListHeading = "%n@|bold,underline Options:|@%n")
    public abstract static class MantaSubCommand {
        @SuppressWarnings("unused")
        public enum CommandLogLevel { TRACE, DEBUG, INFO, WARN, ERROR }

        protected static final String BR = System.lineSeparator();

        protected static final String INDENT = "  ";

        @SuppressWarnings("unused")
        @CommandLine.Option(names = {"-h", "--help"}, help = true)
        private boolean isHelpRequested;

        @SuppressWarnings("unused")
        @CommandLine.Option(names = {"--log-level"},
                            description = "TRACE, DEBUG, INFO, WARN(default), ERROR")
        private CommandLogLevel logLevel;


        public abstract void run();

        /**
         * Builds a new {@link ConfigContext} instance based on defaults and
         * environment variables.
         *
         * @return chained configuration context object
         */
        protected ConfigContext buildConfig() {
            return new ChainedConfigContext(new DefaultsConfigContext(),
                                            new EnvVarConfigContext(),
                                            new MapConfigContext(System.getProperties()));
        }
    }


    @CommandLine.Command(name = "connect-test",
                         header = "Try to connect",
                         description = "Attempts to connect to Manta using system properties "
                         + "and environment variables for configuration")
    public static class ConnectTest extends MantaSubCommand {

        @Override
        public void run() {
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
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            System.out.println(b.toString());
        }
    }


    @CommandLine.Command(name = "dump-config",
                         header = "Dumps the configuration used for configuring a Manta client.")
    public static class DumpConfig extends MantaSubCommand {
        @Override
        public void run() {
            StringBuilder b = new StringBuilder();
            ConfigContext config = buildConfig();
            b.append(ConfigContext.toString(config));
            System.out.println(b.toString());
        }
    }


    @CommandLine.Command(name = "generate-key",
                         header = "Generate an encryption key",
                         description = "Generates a client-side encryption key with the specified "
                         + "cipher and bits at the specified path.")
    public static class GenerateKey extends MantaSubCommand {
        @SuppressWarnings("unused")
        @CommandLine.Parameters(index = "0", description = "cipher to generate key for")
        private String cipher;

        @SuppressWarnings("unused")
        @CommandLine.Parameters(index = "1", description = "number of bits of the key")
        private int bits;

        @SuppressWarnings("unused")
        @CommandLine.Parameters(index = "2", description = "path to write the key to")
        private Path path;

        @Override
        public void run() {
            StringBuilder b = new StringBuilder();

            try {
                b.append("Generating key").append(BR);
                SecretKey key = SecretKeyUtils.generate(cipher, bits);

                b.append(String.format("Writing [%s-%d] key to [%s]", cipher, bits, path));
                SecretKeyUtils.writeKeyToPath(key, path);
            } catch (NoSuchAlgorithmException e) {
                System.err.printf("The running JVM [%s/%s] doesn't support the "
                                  + "supplied cipher name [%s]", System.getProperty("java.version"),
                                  System.getProperty("java.vendor"), cipher);
                System.err.println();
                return;
            } catch (IOException e) {
                String msg = String.format("Unable to write key to path [%s]",
                                           path);
                throw new UncheckedIOException(msg, e);
            }

            System.out.println(b.toString());
        }
    }


    @CommandLine.Command(name = "ls",
                         header = "list",
                         description = "List directory contents")
    public static class ListDir extends MantaSubCommand {
        @SuppressWarnings("unused")
        @CommandLine.Parameters(index = "0", description = "dir to ls")
        private String dirPath;

        @Override
        public void run() {
            final StringBuilder b = new StringBuilder();
            ConfigContext config = buildConfig();
            try (MantaClient client = new MantaClient(config)) {
                final Stream<MantaObject> objs = client.listObjects(dirPath);
                objs.forEach(obj -> b.append(INDENT).append(obj.getPath()).append(BR));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            System.out.println(b.toString());
        }
    }


    @CommandLine.Command(name = "get-file",
                         header = "Performs a download of file in Manta",
                         description = "Performs a download of file in Manta.")
    public static class GetFile extends MantaSubCommand {
        @CommandLine.Option(names = {"-o", "--output"},
                            description = "write output to <file> instead of stdout")
        private String outputFileName;
        @CommandLine.Option(names = {"-O", "--remote-name"},
                            description = "write output to a file using remote object"
                            + "name as filename")
        private boolean inferOutputFileName = false;

        @SuppressWarnings("unused")
        @CommandLine.Parameters(index = "0", description = "Object path in Manta to download")
        private String filePath;

        @Override
        public void run() {
            ConfigContext config = buildConfig();
            try (MantaClient client = new MantaClient(config)) {
                OutputStream out = System.out;
                if (inferOutputFileName) {
                    outputFileName = MantaUtils.lastItemInPath(filePath);
                }
                if (outputFileName != null) {
                    out = new FileOutputStream(outputFileName);
                }

                InputStream objectStream = client.getAsInputStream(filePath);
                IOUtils.copyLarge(objectStream, out);
                objectStream.close();

                out.flush();
                if (outputFileName != null) {
                    out.close();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }


    @CommandLine.Command(name = "put-file",
                         header = "Performs a put of a local file to Manta",
                         description = "Performs a put of a local file to Manta.")
    public static class PutFile extends MantaSubCommand {
        @SuppressWarnings("unused")
        @CommandLine.Parameters(index = "0", description = "file to upload/put")
        private String filePath;

        @SuppressWarnings("unused")
        @CommandLine.Parameters(index = "1", description = "path in Manta to upload to")
        private String mantaPath;

        @Override
        public void run() {
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
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            System.out.println(b.toString());
        }
    }

    @CommandLine.Command(name = "object-info",
                         header = "show HTTP headers for a Manta object",
                         description = "show HTTP headers for a Manta object.")
    public static class ObjectInfo extends MantaSubCommand {
        @SuppressWarnings("unused")
        @CommandLine.Parameters(index = "0", description = "path in Manta to check")
        private String mantaPath;

        @Override
        public void run() {
            ConfigContext config = buildConfig();
            try (MantaClient client = new MantaClient(config)) {
                MantaObjectResponse response = client.head(mantaPath);
                System.out.println(response.toString());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }


    @CommandLine.Command(name = "validate-key",
                         header = "Validate an encryption key",
                         description = "Validates that the supplied key is supported by the "
                         + "SDK's client-side encryption functionality.")
    public static class ValidateKey extends MantaSubCommand {
        @SuppressWarnings("unused")
        @CommandLine.Parameters(index = "0", description = "cipher to validate the key against")
        private String cipher;

        @SuppressWarnings("unused")
        @CommandLine.Parameters(index = "1", description = "path to read the key from")
        private Path path;

        @Override
        public void run() {
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
                return;
            } catch (IOException e) {
                String msg = String.format("Unable to read key from path [%s]",
                                           path);
                throw new UncheckedIOException(msg, e);
            }

            System.out.println(b.toString());
        }
    }

}
