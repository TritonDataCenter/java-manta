/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.test.util.MantaAssert;
import com.joyent.test.util.MantaFunction;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static com.joyent.manta.exception.MantaErrorCode.INVALID_ROLE_TAG_ERROR;
import static com.joyent.manta.util.MantaUtils.asString;

/**
 * Integration tests for verifying the behavior of HTTP header assignment.
 *
 * For the role tests to work properly, you will need to add a <pre>manta</pre>
 * role and a <pre>role2</pre> role.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
@SuppressWarnings("Duplicates")
@Test(groups = { "headers" })
public class MantaHttpHeadersIT {
    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";

    private MantaClient mantaClient;

    private String testPathPrefix;

    private final String primaryRoleName = ObjectUtils.firstNonNull(
            System.getenv("MANTA_IT_ROLE_PRIMARY"),
            System.getProperty("manta.it.role_primary"),
            "primary");

    private final String secondaryRoleName = ObjectUtils.firstNonNull(
            System.getenv("MANTA_IT_ROLE_SECONDARY"),
            System.getProperty("manta.it.role_secondary"),
            "secondary");

    @BeforeClass
    public void beforeClass() throws IOException {

        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new IntegrationTestConfigContext();

        mantaClient = new MantaClient(config);
        testPathPrefix = IntegrationTestConfigContext.generateBasePath(config);
        mantaClient.putDirectory(testPathPrefix, true);
    }

    @AfterClass
    public void cleanup() throws IOException {
        if (mantaClient != null) {
            mantaClient.deleteRecursive(testPathPrefix);
            mantaClient.closeWithWarning();
        }
    }

    @Test
    public void cantSetUnknownTags() throws IOException {
        final MantaHttpHeaders headers = new MantaHttpHeaders();
        final Set<String> roles = new HashSet<>();
        roles.add("thistestprobablydoesntexist");
        headers.setRoles(roles);

        String path = String.format("%s/%s", testPathPrefix, UUID.randomUUID());

        MantaAssert.assertResponseFailureStatusCode(409, INVALID_ROLE_TAG_ERROR,
                (MantaFunction<Object>) () -> mantaClient.put(path, TEST_DATA, headers));
    }

    @Test
    public void canSetSingleRoleTag() throws IOException {
        final MantaHttpHeaders headers = new MantaHttpHeaders();
        final Set<String> roles = new HashSet<>();
        roles.add(primaryRoleName);
        headers.setRoles(roles);

        String path = String.format("%s/%s", testPathPrefix, UUID.randomUUID());

        try {
            mantaClient.put(path, TEST_DATA, headers);
        } catch (MantaClientHttpResponseException e) {
            if (e.getServerCode().equals(INVALID_ROLE_TAG_ERROR)) {
                String msg = "You will need to add roles [manta] in "
                        + "order for this test to work";
                throw new SkipException(msg, e);
            }

            throw e;
        }

        MantaObject object = mantaClient.get(path);
        MantaHttpHeaders actualHeaders = object.getHttpHeaders();
        Set<String> actual = actualHeaders.getRoles();

        if (!CollectionUtils.isEqualCollection(actual, roles)) {
            String msg = String.format("Input and output roles, should be equal.\n" +
                                       "Actual:   [%s]\nExpected: [%s]",
                                       asString(actual),
                                       asString(roles));
            Assert.fail(msg);
        }
    }

    @Test
    public void canSetMultipleRoleTags() throws IOException {
        final MantaHttpHeaders headers = new MantaHttpHeaders();
        final Set<String> roles = new HashSet<>();
        roles.add(primaryRoleName);
        roles.add(secondaryRoleName);
        headers.setRoles(roles);

        String path = String.format("%s/%s", testPathPrefix, UUID.randomUUID());

        try {
            mantaClient.put(path, TEST_DATA, headers);
        } catch (MantaClientHttpResponseException e) {
            if (e.getServerCode().equals(INVALID_ROLE_TAG_ERROR)) {
                String msg = "You will need to add roles [manta, role2] in "
                             + "order for this test to work";
                throw new SkipException(msg, e);
            }

            throw e;
        }

        MantaObject object = mantaClient.get(path);
        MantaHttpHeaders actualHeaders = object.getHttpHeaders();
        Set<String> actual = actualHeaders.getRoles();

        if (!CollectionUtils.isEqualCollection(actual, roles)) {
            String msg = String.format("Input and output roles, should be equal.\n" +
                            "Actual:   [%s]\nExpected: [%s]",
                    asString(actual),
                    asString(roles));
            Assert.fail(msg);
        }
    }

    @Test
    public void canOverwriteRoleTags() throws IOException {
        final MantaHttpHeaders headers = new MantaHttpHeaders();
        final Set<String> roles = new HashSet<>();
        roles.add(primaryRoleName);
        roles.add(secondaryRoleName);
        headers.setRoles(roles);

        String path = String.format("%s/%s", testPathPrefix, UUID.randomUUID());

        try {
            mantaClient.put(path, TEST_DATA, headers);
        } catch (MantaClientHttpResponseException e) {
            if (e.getServerCode().equals(INVALID_ROLE_TAG_ERROR)) {
                String msg = "You will need to add roles [manta, role2] in "
                        + "order for this test to work";
                throw new SkipException(msg, e);
            }
        }

        // Verify that we initially added two roles
        {
            MantaObject object = mantaClient.get(path);
            MantaHttpHeaders actualHeaders = object.getHttpHeaders();
            Set<String> actual = actualHeaders.getRoles();

            if (!CollectionUtils.isEqualCollection(actual, roles)) {
                Assert.fail("Input and output roles, should be equal");
            }
        }

        final Set<String> updatedRoles = new HashSet<>();
        updatedRoles.add("manta");

        final MantaHttpHeaders updatedHeaders = new MantaHttpHeaders();
        updatedHeaders.setRoles(updatedRoles);

        mantaClient.putMetadata(path, updatedHeaders);

        MantaObject object = mantaClient.get(path);
        MantaHttpHeaders actualUpdatedHeaders = object.getHttpHeaders();
        Set<String> actualUpdatedRoles = actualUpdatedHeaders.getRoles();

        if (!CollectionUtils.isEqualCollection(actualUpdatedRoles, updatedRoles)) {
            String msg = String.format("Roles should have been updated.\n" +
                                       "Actual:   [%s]\nExpected: [%s]",
                                       asString(actualUpdatedRoles),
                                       asString(updatedRoles));
            Assert.fail(msg);
        }
    }

    @Test
    public void canClearRoles() throws IOException {
        final MantaHttpHeaders headers = new MantaHttpHeaders();
        final Set<String> roles = new HashSet<>();
        roles.add(primaryRoleName);
        roles.add(secondaryRoleName);
        headers.setRoles(roles);

        String path = String.format("%s/%s", testPathPrefix, UUID.randomUUID());

        try {
            mantaClient.put(path, TEST_DATA, headers);
        } catch (MantaClientHttpResponseException e) {
            if (e.getServerCode().equals(INVALID_ROLE_TAG_ERROR)) {
                String msg = "You will need to add roles [manta, role2] in "
                        + "order for this test to work";
                throw new SkipException(msg, e);
            }
        }

        final Set<String> updatedRoles = new HashSet<>();

        final MantaHttpHeaders updatedHeaders = new MantaHttpHeaders();
        updatedHeaders.setRoles(updatedRoles);

        mantaClient.putMetadata(path, updatedHeaders);

        MantaObject object = mantaClient.get(path);
        MantaHttpHeaders actualUpdatedHeaders = object.getHttpHeaders();
        Set<String> actualUpdatedRoles = actualUpdatedHeaders.getRoles();

        if (!actualUpdatedRoles.isEmpty()) {
            String msg = String.format("Roles weren't removed.\n" +
                                       "Actual:   [%s]\nExpected: []",
                                       asString(actualUpdatedRoles));
            Assert.fail(msg);
        }
    }

    @Test
    public void canSetDurability() throws IOException {
        final int durability = 3;
        final MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setDurabilityLevel(durability);

        String path = String.format("%s/%s", testPathPrefix, UUID.randomUUID());

        mantaClient.put(path, TEST_DATA, headers);

        MantaObject object = mantaClient.get(path);
        MantaHttpHeaders actualHeaders = object.getHttpHeaders();
        Assert.assertEquals(actualHeaders.getDurabilityLevel().intValue(), durability,
                "Durability not set to value on put");
    }
}
