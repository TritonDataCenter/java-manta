/*
 * Copyright (c) 2015-2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/**
 * This package contains classes related to configuring instances of
 * {@link com.joyent.manta.client.MantaClient}.
 *
 * <h3>Design</h3>
 *
 * <p>When a new instance of {@link com.joyent.manta.client.MantaClient} is created
 * it takes a parameter of an instance that implements
 * {@link com.joyent.manta.config.ConfigContext}. Within this package are many
 * implementations of {@link com.joyent.manta.config.ConfigContext}. By allowing
 * for an interface based configuration, we allow users of the SDK to integrate
 * into existing configuration systems in their own applications. One example of
 * this pattern can be seen in the <a href="https://github.com/joyent/hadoop-manta/blob/master/src/main/java/com/joyent/hadoop/fs/manta/HadoopConfigurationContext.java">HadoopConfigurationContext</a>
 * class in the <a href="https://github.com/joyent/hadoop-manta">hadoop-manta
 * project</a>.</p>
 *
 * <p>Many of the classes with {@link com.joyent.manta.config} inherit from the
 * abstract class {@link com.joyent.manta.config.BaseChainedConfigContext}
 * because it supplies methods for allowing you to chain together multiple
 * configuration context implementations such that they will support default
 * values and allow for layered overwrites of settings.</p>
 *
 * <p>For example, the following code would allow you to chain together
 * multiple configuration implementations:</p>
 *
 * <pre>{@code
 * // Creates a composite context from three separate contexts
 * ChainedConfigContext chained = new ChainedConfigContext(
 *     // This context will overwrite both previous contexts if its values are set
 *     new EnvVarConfigContext(),
 *     // This context will overwrite the previous context if its values are set
 *     new MapConfigContext(System.getProperties()),
 *     // This context provides the hardcoded default settings for the Manta client
 *     new DefaultsConfigContext());
 *}</pre>
 */
package com.joyent.manta.config;
