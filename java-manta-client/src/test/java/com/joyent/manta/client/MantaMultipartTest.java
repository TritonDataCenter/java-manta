package com.joyent.manta.client;

import com.joyent.manta.config.ChainedConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.StandardConfigContext;
import com.joyent.manta.config.SystemSettingsConfigContext;
import com.joyent.manta.exception.MantaClientException;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

@Test
public class MantaMultipartTest {
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void negativePartNumbersAreRejected() {
        MantaMultipart.validatePartNumber(-1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void zeroPartNumbersAreRejected() {
        MantaMultipart.validatePartNumber(0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void partNumbersAboveMaxAreRejected() {
        MantaMultipart.validatePartNumber(MantaMultipart.MAX_PARTS + 1);
    }

    public void canBuildMultiPartUploadPath() {
        final UUID id = new UUID(0L, 12L);
        MantaMultipart multipart = multipartInstance("user.name");
        String expected = String.format("/user.name/%s/%s/",
                MantaMultipart.MULTIPART_DIRECTORY, id);
        String actual = multipart.multipartUploadDir(id);
        assertEquals(expected, actual);
    }

    public void noErrorWhenAllPartsArePresentOrdered() throws IOException {
        final UUID id = new UUID(0L, 24L);

        List<Integer> partsList = new LinkedList<>();

        final int totalParts = 64;
        for (int i = 1; i <= totalParts; i++) {
            partsList.add(i);
        }
        MantaMultipart multiPart = spy(multipartInstance());
        when(multiPart.listParts(id)).thenReturn(partsList.stream());

        multiPart.validateThereAreNoMissingParts(id);
    }

    public void noErrorWhenAllPartsArePresentUnordered() throws IOException {
        final UUID id = new UUID(0L, 36L);

        List<Integer> partsList = new LinkedList<>();

        final int totalParts = 64;
        for (int i = 1; i <= totalParts; i++) {
            partsList.add(i);
        }

        Collections.shuffle(partsList);

        MantaMultipart multiPart = spy(multipartInstance());
        when(multiPart.listParts(id)).thenReturn(partsList.stream());

        multiPart.validateThereAreNoMissingParts(id);
    }

    public void errorWhenMissingPart() throws IOException {
        final UUID id = new UUID(0L, 48L);

        ArrayList<Integer> partsList = new ArrayList<>();

        final int totalParts = 64;
        for (int i = 1; i <= totalParts; i++) {
            partsList.add(i);
        }

        partsList.remove(2);

        Collections.shuffle(partsList);

        MantaMultipart multiPart = spy(multipartInstance());
        when(multiPart.listParts(id)).thenReturn(partsList.stream());

        boolean thrown = false;

        try {
            multiPart.validateThereAreNoMissingParts(id);
        } catch (MantaClientException e) {
            if ((int)e.getFirstContextValue("missing_part") == 3) {
                thrown = true;
            }
        }

        assertTrue(thrown, "Exception wasn't thrown");
    }

    private MantaMultipart multipartInstance() {
        return multipartInstance("user.name");
    }

    private MantaMultipart multipartInstance(final String user) {
        final ConfigContext overwrite = new StandardConfigContext()
                .setMantaUser(user);
        final ConfigContext config = new ChainedConfigContext(
                new SystemSettingsConfigContext(),
                overwrite);

        try {
            final MantaClient client = new MantaClient(config);

            return new MantaMultipart(client);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
