/*
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.util;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Date;
import java.util.Properties;

/**
 * Utility class providing the current version of the SDK.
 */
public final class MantaVersion {
    /**
     * Logging instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(MantaVersion.class);

    /**
     * Filename of file to read version data from.
     */
    private static final String VERSION_FILE = "version.properties";

    static {
        final String unknownVersion = "unknown";
        Properties properties = readVersionPropertiesFile();

        if (!properties.isEmpty()) {
            final String versionString = properties.getProperty("version");
            final String dateString = properties.getProperty("date");

            VERSION = ObjectUtils.firstNonNull(versionString, unknownVersion);
            DATE = parseVersionDate(dateString);
        } else {
            LOGGER.warn("Versions properties were empty");
            VERSION = unknownVersion;
            DATE = null;
        }
    }

    /**
     * No public constructor because only static methods are exposed.
     */
    private MantaVersion() {
    }

    /**
     * Release version of the SDK.
     */
    public static final String VERSION;

    /**
     * Release date of the SDK.
     */
    public static final Date DATE;

    /**
     * Loads properties from version properties file into a {@link Properties}
     * object.
     *
     * @return populated properties object
     */
    private static Properties readVersionPropertiesFile() {
        final Properties properties = new Properties();

        try (InputStream in = ClassLoader.getSystemResourceAsStream(VERSION_FILE)) {
            properties.load(in);
        } catch (IOException e) {
            String msg = String.format("Unable to read version data file from class path."
                    + " Filename: %s", VERSION_FILE);
            LOGGER.warn(msg, e);
        }

        return properties;
    }

    /**
     * Parses version string into a {@link Date} object.
     * @param dateString null or date string in dd-MM-yyyy HH-mm-ss format
     * @return date object or null
     */
    private static Date parseVersionDate(final String dateString) {
        if (dateString == null) {
            return null;
        }

        Date date;

        try {
            date = DateUtils.parseDate(dateString, "dd-MM-yyyy HH-mm-ss");
        } catch (ParseException e) {
            String msg = String.format("Unable to parse date string: %s",
                    dateString);
            LOGGER.warn(msg, e);
            date = null;
        }

        return date;
    }
}
