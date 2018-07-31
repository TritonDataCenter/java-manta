package com.joyent.manta.client;

import com.joyent.manta.config.TestConfigContext;
import com.joyent.manta.http.HttpHelper;
import com.joyent.manta.http.MantaHttpHeaders;
import org.mockito.Mockito;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Test
public class MantaClientTest {

    @AfterTest
    public void teardown() {
        Mockito.validateMockitoUsage();
    }

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

    public void deleteWithHeadersPassesAlongHeaders() throws IOException {
        final HttpHelper helper = mock(HttpHelper.class);
        final MantaClient client = new MantaClient(new TestConfigContext(), null, helper);
        final MantaHttpHeaders hdrs = new MantaHttpHeaders();
        final String etag = "magic";
        hdrs.setIfMatch(etag);

        client.delete("/test/stor/foo", hdrs);

        verify(helper).httpDelete(anyString(), argThat(h -> etag.equals(h.getIfMatch())));
    }
}
