/*
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.cli;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.config.ChainedConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.EnvVarConfigContext;
import com.joyent.manta.config.MapConfigContext;
import com.joyent.manta.util.MantaVersion;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;

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
                case "help":
                default:
                    System.out.println(help());
            }
        } catch (Exception e) {
            System.err.println(e.getLocalizedMessage());
            System.err.println(ExceptionUtils.getStackTrace(e));
        }
    }

    /**
     * Displays help for the CLI.
     * @return string containing help text
     */
    public static String help() {
        StringBuilder b = new StringBuilder();

        String jar = String.format("java-manta-client-%s(-with-dependencies).jar", MantaVersion.VERSION);

        b.append("Java Manta SDK").append(BR)
         .append(MantaVersion.VERSION).append(" ").append(MantaVersion.DATE).append(BR)
         .append("========================================").append(BR)
         .append(BR)
         .append("java -jar ").append(jar).append(" <command>").append(BR)
         .append(BR)
         .append("Commands:").append(BR)
         .append(INDENT).append("connect-test        ")
                        .append("Attempts to connect to Manta using system properties and environment variables for configuration.").append(BR)
         .append(INDENT).append("dump-config         ")
                        .append("Dumps the configuration that was loaded using defaults, system properties and environment variables").append(BR)
         .append(INDENT).append("help                ")
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
