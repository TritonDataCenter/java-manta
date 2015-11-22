package com.joyent.manta.client;

import com.joyent.manta.client.config.TestConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.test.util.MantaAssert;
import com.joyent.test.util.MantaFunction;
import junit.framework.Assert;
import org.apache.commons.collections4.CollectionUtils;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static com.joyent.manta.exception.MantaErrorCode.INVALID_ROLE_TAG_ERROR;

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


    @BeforeClass
    @Parameters({"manta.url", "manta.user", "manta.key_path", "manta.key_id", "manta.timeout"})
    public void beforeClass(@Optional String mantaUrl,
                            @Optional String mantaUser,
                            @Optional String mantaKeyPath,
                            @Optional String mantaKeyId,
                            @Optional Integer mantaTimeout)
            throws IOException {

        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new TestConfigContext(
                mantaUrl, mantaUser, mantaKeyPath, mantaKeyId, mantaTimeout);

        mantaClient = new MantaClient(config);
        testPathPrefix = String.format("/%s/stor/%s/",
                config.getMantaHomeDirectory(), UUID.randomUUID());
        mantaClient.putDirectory(testPathPrefix, null);
    }


    @AfterClass
    public void cleanup() throws IOException {
        if (mantaClient != null) {
            mantaClient.deleteRecursive(testPathPrefix);
            mantaClient.closeQuietly();
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
        roles.add("manta");
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
            Assert.failNotEquals("Input and output roles, should be equal",
                    roles, actual);
        }
    }


    @Test
    public void canSetMultipleRoleTags() throws IOException {
        final MantaHttpHeaders headers = new MantaHttpHeaders();
        final Set<String> roles = new HashSet<>();
        roles.add("manta");
        roles.add("role2");
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
            Assert.failNotEquals("Input and output roles, should be equal",
                    roles, actual);
        }
    }

    @Test
    public void canOverwriteRoleTags() throws IOException {
        final MantaHttpHeaders headers = new MantaHttpHeaders();
        final Set<String> roles = new HashSet<>();
        roles.add("manta");
        roles.add("role2");
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
                Assert.failNotEquals("Input and output roles, should be equal",
                        roles, actual);
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
            Assert.failNotEquals("Roles should have been updated",
                    roles, actualUpdatedRoles);
        }
    }

    @Test
    public void canClearRoles() throws IOException {
        final MantaHttpHeaders headers = new MantaHttpHeaders();
        final Set<String> roles = new HashSet<>();
        roles.add("manta");
        roles.add("role2");
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
            Assert.failNotEquals("Roles weren't removed", Collections.emptySet(),
                    actualUpdatedRoles);
        }
    }
}
