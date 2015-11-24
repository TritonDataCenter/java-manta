package com.joyent.manta.client;

import org.apache.commons.collections4.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Tests for verifying the behavior of HTTP header assignment.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
@Test(groups = { "headers" })
public class MantaHttpHeadersTest {
    @Test
    public void canSetRoleTags() {
        final MantaHttpHeaders headers = new MantaHttpHeaders();
        final Set<String> roles = new HashSet<>();
        roles.add("admin");
        roles.add("read-only");
        roles.add("reporting");

        headers.setRoles(roles);

        Set<String> actual = headers.getRoles();

        if (!CollectionUtils.isEqualCollection(actual, roles)) {
            Assert.fail("Input and output roles, should be equal");
        }
    }
}
