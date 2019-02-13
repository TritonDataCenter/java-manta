package com.joyent.manta.client;

import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.TestConfigContext;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaErrorCode;
import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.commons.lang3.ObjectUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.joyent.manta.client.PruneEmptyParentDirectoryStrategy.PRUNE_ALL_PARENTS;
import static com.joyent.manta.client.PruneEmptyParentDirectoryStrategy.extractOrderedWriteableAncestorPaths;
import static com.joyent.manta.client.PruneEmptyParentDirectoryStrategy.pruneParentDirectories;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;

@Test
public class PruneEmptyParentDirectoryStrategyTest {
    private MantaClient client;

    @BeforeClass
    public void beforeClass() {
        this.client = new MantaClient(new TestConfigContext());
    }

    @AfterClass
    public void afterClass() {
        this.client.closeWithWarning();
    }

    public void canParseObjectPath() {
        final String path = "/user/stor/dir1/dir2/dir3/object.txt";
        final String[] expected = new String[] {
                "/user/stor/dir1/dir2/dir3",
                "/user/stor/dir1/dir2",
                "/user/stor/dir1"
        };
        final String[] directories = extractOrderedWriteableAncestorPaths(path);

        Assert.assertEquals(directories, expected);
    }

    public void canParseObjectPathWithNotNormalizedSeparators() {
        final String path = "/user/stor/dir1//dir2///dir3/object.txt";
        final String[] expected = new String[] {
                "/user/stor/dir1/dir2/dir3",
                "/user/stor/dir1/dir2",
                "/user/stor/dir1"
        };
        final String[] directories = extractOrderedWriteableAncestorPaths(path);

        Assert.assertEquals(directories, expected);
    }

    public void canParseDirectoryPath() {
        final String path = "/user/stor/dir1/dir2/dir3/dir4/";
        final String[] expected = new String[] {
                "/user/stor/dir1/dir2/dir3",
                "/user/stor/dir1/dir2",
                "/user/stor/dir1"
        };
        final String[] directories = extractOrderedWriteableAncestorPaths(path);

        Assert.assertEquals(directories, expected);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void willErrorWithALimitBelowMinusOne() throws IOException {
        final MantaClient clientSpy = spy(client);
        final MantaHttpHeaders headers = new MantaHttpHeaders();
        final String path = "/user/stor/dir1/dir2/object.txt";

        // Prevent the client from actually calling out to a real Manta
        Mockito.doNothing().when(clientSpy)
                .delete(startsWith("/user/stor"), any(MantaHttpHeaders.class), isNull());

        pruneParentDirectories(clientSpy, headers, path, -2);
    }

    public void canPruneEmptyDirectoriesWithDefaultSetting() throws IOException {
        final MantaClient clientSpy = spy(client);
        final MantaHttpHeaders headers = new MantaHttpHeaders();
        final String path = "/user/stor/dir1/dir2/object.txt";
        final DefaultsConfigContext config = new DefaultsConfigContext();

        /* We look for the value of the defaults as returned by the default
         * method in the default configuration settings class first because
         * it could be changed to be something other than null. If it is null,
         * then we get the value that the null value is interpreted as. We pull
         * the limit value in this way in order to validate that our test logic
         * matches our app logic.
         */
        final int limit = ObjectUtils.firstNonNull(config.getPruneEmptyParentDepth(),
                DefaultsConfigContext.DEFAULT_PRUNE_DEPTH);

        // Prevent the client from actually calling out to a real Manta
        Mockito.doNothing().when(clientSpy)
                .delete(startsWith("/user/stor"), any(MantaHttpHeaders.class), isNull());

        pruneParentDirectories(clientSpy, headers, path, limit);

        // Verify that we only issued a single delete call because our limit is 0
        Mockito.verify(clientSpy, times(1))
                .delete(path, headers, null);

        // Verify that is the only call that we made
        Mockito.verifyNoMoreInteractions(clientSpy);
    }

    public void canPruneEmptyDirectoriesWithLimitOne() throws IOException {
        final MantaClient clientSpy = spy(client);
        final MantaHttpHeaders headers = new MantaHttpHeaders();
        final String path = "/user/stor/dir1/dir2/object.txt";
        final int limit = 1;

        // Prevent the client from actually calling out to a real Manta
        Mockito.doNothing().when(clientSpy)
                .delete(startsWith("/user/stor"), any(MantaHttpHeaders.class), isNull());

        pruneParentDirectories(clientSpy, headers, path, limit);

        Mockito.verify(clientSpy, times(1))
                .delete(path, headers, null);
        Mockito.verify(clientSpy, times(1))
                .delete("/user/stor/dir1/dir2", headers, null);

        // Verify that is the only call that we made
        Mockito.verifyNoMoreInteractions(clientSpy);
    }

    public void canPruneEmptyDirectoriesWithLimitTwo() throws IOException {
        final MantaClient clientSpy = spy(client);
        final MantaHttpHeaders headers = new MantaHttpHeaders();
        final String path = "/user/stor/dir1/dir2/object.txt";
        final int limit = 2;

        // Prevent the client from actually calling out to a real Manta
        Mockito.doNothing().when(clientSpy)
                .delete(startsWith("/user/stor"), any(MantaHttpHeaders.class), isNull());

        pruneParentDirectories(clientSpy, headers, path, limit);

        Mockito.verify(clientSpy, times(1))
                .delete(path, headers, null);
        Mockito.verify(clientSpy, times(1))
                .delete("/user/stor/dir1/dir2", headers, null);
        Mockito.verify(clientSpy, times(1))
                .delete("/user/stor/dir1", headers, null);

        // Verify that is the only call that we made
        Mockito.verifyNoMoreInteractions(clientSpy);
    }

    public void canPruneEmptyDirectoriesWithLimitFour() throws IOException {
        final MantaClient clientSpy = spy(client);
        final MantaHttpHeaders headers = new MantaHttpHeaders();
        final String path = "/user/stor/dir1/dir2/dir3/dir4/object.txt";
        final int limit = 4;

        // Prevent the client from actually calling out to a real Manta
        Mockito.doNothing().when(clientSpy)
                .delete(startsWith("/user/stor"), any(MantaHttpHeaders.class), isNull());

        pruneParentDirectories(clientSpy, headers, path, limit);

        Mockito.verify(clientSpy, times(1))
                .delete(path, headers, null);
        Mockito.verify(clientSpy, times(1))
                .delete("/user/stor/dir1/dir2/dir3/dir4", headers, null);
        Mockito.verify(clientSpy, times(1))
                .delete("/user/stor/dir1/dir2/dir3", headers, null);
        Mockito.verify(clientSpy, times(1))
                .delete("/user/stor/dir1/dir2", headers, null);
        Mockito.verify(clientSpy, times(1))
                .delete("/user/stor/dir1", headers, null);

        // Verify that is the only call that we made
        Mockito.verifyNoMoreInteractions(clientSpy);
    }

    public void verifyThatPruneEmptyDirectoriesWontPruneBelowStorRoot() throws IOException {
        final MantaClient clientSpy = spy(client);
        final MantaHttpHeaders headers = new MantaHttpHeaders();
        final String path = "/user/stor/dir1/dir2/object.txt";
        final int limit = 3;

        // Prevent the client from actually calling out to a real Manta
        Mockito.doNothing().when(clientSpy)
                .delete(startsWith("/user/stor"), any(MantaHttpHeaders.class), isNull());

        pruneParentDirectories(clientSpy, headers, path, limit);

        Mockito.verify(clientSpy, times(1))
                .delete(path, headers, null);
        Mockito.verify(clientSpy, times(1))
                .delete("/user/stor/dir1/dir2", headers, null);
        Mockito.verify(clientSpy, times(1))
                .delete("/user/stor/dir1", headers, null);

        // Verify that is the only call that we made
        Mockito.verifyNoMoreInteractions(clientSpy);
    }

    public void canPruneAllDirectories() throws IOException {
        final MantaClient clientSpy = spy(client);
        final MantaHttpHeaders headers = new MantaHttpHeaders();
        final String path = "/user/stor/dir1/dir2/object.txt";
        final int limit = PRUNE_ALL_PARENTS;

        // Prevent the client from actually calling out to a real Manta
        Mockito.doNothing().when(clientSpy)
                .delete(startsWith("/user/stor"), any(MantaHttpHeaders.class), isNull());

        pruneParentDirectories(clientSpy, headers, path, limit);

        Mockito.verify(clientSpy, times(1))
                .delete(path, headers, null);
        Mockito.verify(clientSpy, times(1))
                .delete("/user/stor/dir1/dir2", headers, null);
        Mockito.verify(clientSpy, times(1))
                .delete("/user/stor/dir1", headers, null);

        // Verify that is the only call that we made
        Mockito.verifyNoMoreInteractions(clientSpy);
    }

    public void willStopPruningWhenDirectoryHasContents() throws IOException {
        final MantaClient clientSpy = spy(client);
        final MantaHttpHeaders headers = new MantaHttpHeaders();
        final String path = "/user/stor/dir1/dir2/dir3/object.txt";
        final int limit = PRUNE_ALL_PARENTS;

        final MantaClientHttpResponseException exception =
                new MantaClientHttpResponseException("Unit test fake error")
                .setServerCode(MantaErrorCode.DIRECTORY_NOT_EMPTY_ERROR);

        // Prevent the client from actually calling out to a real Manta
        Mockito.doNothing().when(clientSpy)
                .delete(startsWith("/user/stor/dir1/dir2/dir3"), any(MantaHttpHeaders.class), isNull());

        Mockito.doThrow(exception).when(clientSpy)
                .delete(matches("^(?!\\/user\\/stor\\/dir1\\/dir2\\/dir3).+"), any(MantaHttpHeaders.class), isNull());

        pruneParentDirectories(clientSpy, headers, path, limit);

        Mockito.verify(clientSpy, times(1))
                .delete(path, headers, null);
        Mockito.verify(clientSpy, times(1))
                .delete("/user/stor/dir1/dir2/dir3", headers, null);
        Mockito.verify(clientSpy, times(1))
                .delete("/user/stor/dir1/dir2", headers, null);
        Mockito.verify(clientSpy, never())
                .delete("/user/stor/dir1", headers, null);

        // Verify that is the only call that we made
        Mockito.verifyNoMoreInteractions(clientSpy);
    }

    public void willStopPruningWhenDirectoryIsNotFound() throws IOException {
        final MantaClient clientSpy = spy(client);
        final MantaHttpHeaders headers = new MantaHttpHeaders();
        final String path = "/user/stor/dir1/dir2/dir3/object.txt";
        final int limit = PRUNE_ALL_PARENTS;

        final MantaClientHttpResponseException exception =
                new MantaClientHttpResponseException("Unit test fake error")
                        .setServerCode(MantaErrorCode.RESOURCE_NOT_FOUND_ERROR);

        // Prevent the client from actually calling out to a real Manta
        Mockito.doNothing().when(clientSpy)
                .delete(startsWith("/user/stor/dir1/dir2/dir3"), any(MantaHttpHeaders.class), isNull());

        Mockito.doThrow(exception).when(clientSpy)
                .delete(matches("^(?!\\/user\\/stor\\/dir1\\/dir2\\/dir3).+"), any(MantaHttpHeaders.class), isNull());

        pruneParentDirectories(clientSpy, headers, path, limit);

        Mockito.verify(clientSpy, times(1))
                .delete(path, headers, null);
        Mockito.verify(clientSpy, times(1))
                .delete("/user/stor/dir1/dir2/dir3", headers, null);
        Mockito.verify(clientSpy, times(1))
                .delete("/user/stor/dir1/dir2", headers, null);
        Mockito.verify(clientSpy, never())
                .delete("/user/stor/dir1", headers, null);

        // Verify that is the only call that we made
        Mockito.verifyNoMoreInteractions(clientSpy);
    }
}
