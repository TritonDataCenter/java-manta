package com.joyent.manta.client;

import com.joyent.manta.client.helper.IntegrationTestHelper;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaErrorCode;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.UUID;

import static com.joyent.manta.client.MantaClient.SEPARATOR;

/**
 * Tests the basic move directory functionality of the {@link MantaClient} class.
 */
@Test(groups = {"move"})
public class MantaClientMoveDirectoryIT {

    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";

    private final MantaClient mantaClient;

    private final String testPathPrefix;

    public MantaClientMoveDirectoryIT(final @Optional String encryptionCipher,
                                      final @Optional String testType) throws IOException {

        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new IntegrationTestConfigContext(encryptionCipher);
        final String testName = this.getClass().getSimpleName();

        mantaClient = new MantaClient(config);
        testPathPrefix = IntegrationTestHelper.setupTestPath(config, mantaClient,
                testName, testType);
    }

    @BeforeClass
    @Parameters({"testType"})
    public void beforeClass(final @Optional String testType) throws IOException {
        if ("buckets".equals(testType)) {
            throw new SkipException("Directory tests will be skipped in Manta Buckets");
        }

        IntegrationTestHelper.createTestBucketOrDirectory(mantaClient, testPathPrefix, testType);
    }

    @AfterClass
    public void afterClass() throws IOException {
        IntegrationTestHelper.cleanupTestBucketOrDirectory(mantaClient, testPathPrefix);
    }

    @Test
    public final void canMoveFileToDifferentUncreatedDirectoryCreationDisabled() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        mantaClient.put(path, TEST_DATA);
        final String newDir = testPathPrefix + "subdir-" + UUID.randomUUID() + SEPARATOR;

        final String newPath = newDir + "this-is-a-new-name.txt";

        boolean thrown = false;

        try {
            mantaClient.move(path, newPath, false);
        } catch (MantaClientHttpResponseException e) {
            String serverCode = e.getContextValues("serverCode").get(0).toString();

            if (serverCode.equals(MantaErrorCode.DIRECTORY_DOES_NOT_EXIST_ERROR.getCode())) {
                thrown = true;
            }
        }

        Assert.assertTrue(thrown, "Expected exception [MantaClientHttpResponseException] wasn't thrown");
    }

    @Test
    public final void canMoveEmptyDirectory() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        mantaClient.putDirectory(path);
        final String newDir = testPathPrefix + "subdir-" + UUID.randomUUID() + SEPARATOR;

        mantaClient.move(path, newDir);
        boolean newLocationExists = mantaClient.existsAndIsAccessible(newDir);
        Assert.assertTrue(newLocationExists, "Destination directory doesn't exist: "
                + newDir);

        MantaObjectResponse head = mantaClient.head(newDir);

        boolean isDirectory = head.isDirectory();
        Assert.assertTrue(isDirectory, "Destination wasn't created as a directory");

        Long resultSetSize = head.getHttpHeaders().getResultSetSize();
        Assert.assertEquals(resultSetSize.longValue(), 0L,
                "Destination directory is not empty");

        boolean sourceIsDeleted = !mantaClient.existsAndIsAccessible(path);
        Assert.assertTrue(sourceIsDeleted, "Source directory didn't get deleted: "
                + path);
    }

    private void moveDirectoryWithContents(final String source, final String destination) throws IOException {
        mantaClient.putDirectory(source);

        mantaClient.putDirectory(source + "dir1");
        mantaClient.putDirectory(source + "dir2");
        mantaClient.putDirectory(source + "dir3");

        mantaClient.put(source + "file1.txt", TEST_DATA);
        mantaClient.put(source + "file2.txt", TEST_DATA);
        mantaClient.put(source + "dir1/file3.txt", TEST_DATA);
        mantaClient.put(source + "dir1/file4.txt", TEST_DATA);
        mantaClient.put(source + "dir3/file5.txt", TEST_DATA);

        mantaClient.move(source, destination);
        boolean newLocationExists = mantaClient.existsAndIsAccessible(destination);
        Assert.assertTrue(newLocationExists, "Destination directory doesn't exist: "
                + destination);

        MantaObjectResponse headDestination = mantaClient.head(destination);

        boolean isDirectory = headDestination.isDirectory();
        Assert.assertTrue(isDirectory, "Destination wasn't created as a directory");

        Long resultSetSize = headDestination.getHttpHeaders().getResultSetSize();
        Assert.assertEquals(resultSetSize.longValue(), 5L,
                "Destination directory doesn't have the same number of entries as source");

        Assert.assertTrue(mantaClient.existsAndIsAccessible(destination + "file1.txt"));
        Assert.assertTrue(mantaClient.existsAndIsAccessible(destination + "file2.txt"));
        Assert.assertTrue(mantaClient.existsAndIsAccessible(destination + "dir1"));
        Assert.assertTrue(mantaClient.existsAndIsAccessible(destination + "dir2"));
        Assert.assertTrue(mantaClient.existsAndIsAccessible(destination + "dir3"));
        Assert.assertTrue(mantaClient.existsAndIsAccessible(destination + "dir1/file3.txt"));
        Assert.assertTrue(mantaClient.existsAndIsAccessible(destination + "dir1/file4.txt"));
        Assert.assertTrue(mantaClient.existsAndIsAccessible(destination + "dir3/file5.txt"));

        boolean sourceIsDeleted = !mantaClient.existsAndIsAccessible(source);
        Assert.assertTrue(sourceIsDeleted, "Source directory didn't get deleted: "
                + source);
    }

    @Test(enabled = false) // Triggers server side bug: MANTA-2409
    public final void canMoveDirectoryWithContentsAndErrorProneCharacters() throws IOException {
        final String name = "source-" + UUID.randomUUID().toString() + "- -!@#$%^&*()";
        final String source = testPathPrefix + name + MantaClient.SEPARATOR;
        final String destination = testPathPrefix + "dest-" + UUID.randomUUID() + "- -!@#$%^&*" + SEPARATOR;
        moveDirectoryWithContents(source, destination);
    }
}
