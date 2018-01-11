package com.joyent.manta.client;

import com.joyent.manta.config.TestConfigContext;
import org.mockito.Mockito;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MantaClientTest {

    @AfterTest
    public void teardown() {
        Mockito.validateMockitoUsage();
    }

    @Test
    public void listObjectsDoesNotLeakConnectionsWhenThereAreResults() throws IOException {
        // BasicHttpClientConnectionManager maintains a single connection

        final MantaDirectoryListingIterator iteratorMock = mock(MantaDirectoryListingIterator.class);
        when(iteratorMock.hasNext()).thenReturn(true);

        final MantaClient client = new MantaClient(new TestConfigContext());
        final MantaClient clientSpy = spy(client);
        doReturn(iteratorMock).when(clientSpy).streamingIterator(anyString());

        final Stream<MantaObject> listing = clientSpy.listObjects("/");
        listing.close();

        verify(iteratorMock).close();
    }

    @Test
    public void listObjectsDoesNotLeakConnectionsWhenNoResults() throws IOException {
        // BasicHttpClientConnectionManager maintains a single connection

        final MantaDirectoryListingIterator iteratorMock = mock(MantaDirectoryListingIterator.class);
        when(iteratorMock.hasNext()).thenReturn(false);

        final MantaClient client = new MantaClient(new TestConfigContext());
        final MantaClient clientSpy = spy(client);
        doReturn(iteratorMock).when(clientSpy).streamingIterator(anyString());

        final Stream<MantaObject> listing = clientSpy.listObjects("/");
        listing.close();

        verify(iteratorMock).close();
    }
}
