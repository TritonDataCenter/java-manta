package com.joyent.manta.client;

import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.TestConfigContext;
import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.commons.lang3.ObjectUtils;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.joyent.manta.client.PruneEmptyParentDirectoryStrategy.PRUNE_ALL_PARENTS;
import static com.joyent.manta.client.PruneEmptyParentDirectoryStrategy.pruneParentDirectories;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@Test
public class PruneEmptyParentDirectoryStrategyTest {
    public void canPruneEmptyDirectoriesWithDefaultSetting() throws IOException {
        final MantaClient client = new MantaClient(new TestConfigContext());
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
        Mockito.doNothing().when(clientSpy).delete(path, headers, null);

        pruneParentDirectories(clientSpy, headers, path, limit);

        // Verify that we only issued a single delete call because our limit is 0
        Mockito.verify(clientSpy, times(1))
                .delete(path, headers, null);

        // Verify that is the only call that we made
        Mockito.verifyNoMoreInteractions(clientSpy);
    }

    public void canPruneEmptyDirectoriesWithLimitOne() throws IOException {
        final MantaClient client = new MantaClient(new TestConfigContext());
        final MantaClient clientSpy = spy(client);
        final MantaHttpHeaders headers = new MantaHttpHeaders();
        final String path = "/user/stor/dir1/dir2/object.txt";
        final int limit = 1;

        // Prevent the client from actually calling out to a real Manta
        Mockito.doNothing().when(clientSpy).delete(path, headers, null);
        Mockito.doNothing().when(clientSpy).delete("/user/stor/dir1/dir2", headers, null);

        pruneParentDirectories(clientSpy, headers, path, limit);

        Mockito.verify(clientSpy, times(1))
                .delete(path, headers, null);
        Mockito.verify(clientSpy, times(1))
                .delete("/user/stor/dir1/dir2", headers, null);

        // Verify that is the only call that we made
        Mockito.verifyNoMoreInteractions(clientSpy);
    }

    public void canPruneEmptyDirectoriesWithLimitTwo() throws IOException {
        final MantaClient client = new MantaClient(new TestConfigContext());
        final MantaClient clientSpy = spy(client);
        final MantaHttpHeaders headers = new MantaHttpHeaders();
        final String path = "/user/stor/dir1/dir2/object.txt";
        final int limit = 2;

        // Prevent the client from actually calling out to a real Manta
        Mockito.doNothing().when(clientSpy).delete(path, headers, null);
        Mockito.doNothing().when(clientSpy).delete("/user/stor/dir1/dir2", headers, null);
        Mockito.doNothing().when(clientSpy).delete("/user/stor/dir1", headers, null);

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

    public void verifyThatPruneEmptyDirectoriesWontPruneBelowStorRoot() throws IOException {
        final MantaClient client = new MantaClient(new TestConfigContext());
        final MantaClient clientSpy = spy(client);
        final MantaHttpHeaders headers = new MantaHttpHeaders();
        final String path = "/user/stor/dir1/dir2/object.txt";
        final int limit = 3;

        // Prevent the client from actually calling out to a real Manta
        Mockito.doNothing().when(clientSpy).delete(path, headers, null);
        Mockito.doNothing().when(clientSpy).delete("/user/stor/dir1/dir2", headers, null);
        Mockito.doNothing().when(clientSpy).delete("/user/stor/dir1", headers, null);

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
        final MantaClient client = new MantaClient(new TestConfigContext());
        final MantaClient clientSpy = spy(client);
        final MantaHttpHeaders headers = new MantaHttpHeaders();
        final String path = "/user/stor/dir1/dir2/object.txt";
        final int limit = PRUNE_ALL_PARENTS;

        // Prevent the client from actually calling out to a real Manta
        Mockito.doNothing().when(clientSpy).delete(path, headers, null);
        Mockito.doNothing().when(clientSpy).delete("/user/stor/dir1/dir2", headers, null);
        Mockito.doNothing().when(clientSpy).delete("/user/stor/dir1", headers, null);

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
}
