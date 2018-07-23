package com.joyent.manta.client.crypto;

import org.testng.annotations.Test;

import java.io.IOException;

import static com.joyent.manta.util.FailingInputStream.FailureOrder.ON_EOF;
import static com.joyent.manta.util.FailingInputStream.FailureOrder.POST_READ;
import static com.joyent.manta.util.FailingInputStream.FailureOrder.PRE_READ;

@Test
public class MantaEncryptedObjectInputStreamAutoFailureRecoveryTest extends AbstractMantaEncryptedObjectInputStreamTest {

    // DECRYPT ENTIRE OBJECT w/ CONTINUATIONS

    public void canDecryptEntireObjectAuthenticatedWithFailureAutoRecoveryAesCbc128() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailureAutoRecovery(AesCbcCipherDetails.INSTANCE_128_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectAuthenticatedWithFailureAutoRecoveryAesCbc192() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailureAutoRecovery(AesCbcCipherDetails.INSTANCE_192_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectAuthenticatedWithFailureAutoRecoveryAesCbc256() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailureAutoRecovery(AesCbcCipherDetails.INSTANCE_256_BIT, true);
    }

    public void canDecryptEntireObjectAuthenticatedWithFailureAutoRecoveryAesCtr128() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailureAutoRecovery(AesCtrCipherDetails.INSTANCE_128_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectAuthenticatedWithFailureAutoRecoveryAesCtr192() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailureAutoRecovery(AesCtrCipherDetails.INSTANCE_192_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectAuthenticatedWithFailureAutoRecoveryAesCtr256() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailureAutoRecovery(AesCtrCipherDetails.INSTANCE_256_BIT, true);
    }

    public void canDecryptEntireObjectAuthenticatedWithFailureAutoRecoveryAesGcm128() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailureAutoRecovery(AesGcmCipherDetails.INSTANCE_128_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectAuthenticatedWithFailureAutoRecoveryAesGcm192() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailureAutoRecovery(AesGcmCipherDetails.INSTANCE_192_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectAuthenticatedWithFailureAutoRecoveryAesGcm256() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailureAutoRecovery(AesGcmCipherDetails.INSTANCE_256_BIT, true);
    }


    public void canDecryptEntireObjectUnauthenticatedWithFailureAutoRecoveryAesCbc128() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailureAutoRecovery(AesCbcCipherDetails.INSTANCE_128_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectUnauthenticatedWithFailureAutoRecoveryAesCbc192() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailureAutoRecovery(AesCbcCipherDetails.INSTANCE_192_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectUnauthenticatedWithFailureAutoRecoveryAesCbc256() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailureAutoRecovery(AesCbcCipherDetails.INSTANCE_256_BIT, false);
    }

    public void canDecryptEntireObjectUnauthenticatedWithFailureAutoRecoveryAesCtr128() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailureAutoRecovery(AesCtrCipherDetails.INSTANCE_128_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectUnauthenticatedWithFailureAutoRecoveryAesCtr192() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailureAutoRecovery(AesCtrCipherDetails.INSTANCE_192_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectUnauthenticatedWithFailureAutoRecoveryAesCtr256() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailureAutoRecovery(AesCtrCipherDetails.INSTANCE_256_BIT, false);
    }

    public void canDecryptEntireObjectUnauthenticatedWithFailureAutoRecoveryAesGcm128() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailureAutoRecovery(AesGcmCipherDetails.INSTANCE_128_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectUnauthenticatedWithFailureAutoRecoveryAesGcm192() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailureAutoRecovery(AesGcmCipherDetails.INSTANCE_192_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectUnauthenticatedWithFailureAutoRecoveryAesGcm256() throws IOException {
        canDecryptEntireObjectAllReadModesWithFailureAutoRecovery(AesGcmCipherDetails.INSTANCE_256_BIT, false);
    }

    // SKIPS w/ CONTINUATIONS

    public void canSkipBytesAuthenticatedWithFailureAutoRecoveryAesCbc128() throws IOException {
        canSkipBytesWithFailureAutoRecovery(AesCbcCipherDetails.INSTANCE_128_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesAuthenticatedWithFailureAutoRecoveryAesCbc192() throws IOException {
        canSkipBytesWithFailureAutoRecovery(AesCbcCipherDetails.INSTANCE_192_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesAuthenticatedWithFailureAutoRecoveryAesCbc256() throws IOException {
        canSkipBytesWithFailureAutoRecovery(AesCbcCipherDetails.INSTANCE_256_BIT, true);
    }

    public void canSkipBytesAuthenticatedWithFailureAutoRecoveryAesCtr128() throws IOException {
        canSkipBytesWithFailureAutoRecovery(AesCtrCipherDetails.INSTANCE_128_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesAuthenticatedWithFailureAutoRecoveryAesCtr192() throws IOException {
        canSkipBytesWithFailureAutoRecovery(AesCtrCipherDetails.INSTANCE_192_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesAuthenticatedWithFailureAutoRecoveryAesCtr256() throws IOException {
        canSkipBytesWithFailureAutoRecovery(AesCtrCipherDetails.INSTANCE_256_BIT, true);
    }

    public void canSkipBytesAuthenticatedWithFailureAutoRecoveryAesGcm128() throws IOException {
        canSkipBytesWithFailureAutoRecovery(AesGcmCipherDetails.INSTANCE_128_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesAuthenticatedWithFailureAutoRecoveryAesGcm192() throws IOException {
        canSkipBytesWithFailureAutoRecovery(AesGcmCipherDetails.INSTANCE_192_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesAuthenticatedWithFailureAutoRecoveryAesGcm256() throws IOException {
        canSkipBytesWithFailureAutoRecovery(AesGcmCipherDetails.INSTANCE_256_BIT, true);
    }


    public void canSkipBytesUnauthenticatedWithFailureAutoRecoveryAesCbc128() throws IOException {
        canSkipBytesWithFailureAutoRecovery(AesCbcCipherDetails.INSTANCE_128_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesUnauthenticatedWithFailureAutoRecoveryAesCbc192() throws IOException {
        canSkipBytesWithFailureAutoRecovery(AesCbcCipherDetails.INSTANCE_192_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesUnauthenticatedWithFailureAutoRecoveryAesCbc256() throws IOException {
        canSkipBytesWithFailureAutoRecovery(AesCbcCipherDetails.INSTANCE_256_BIT, false);
    }

    public void canSkipBytesUnauthenticatedWithFailureAutoRecoveryAesCtr128() throws IOException {
        canSkipBytesWithFailureAutoRecovery(AesCtrCipherDetails.INSTANCE_128_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesUnauthenticatedWithFailureAutoRecoveryAesCtr192() throws IOException {
        canSkipBytesWithFailureAutoRecovery(AesCtrCipherDetails.INSTANCE_192_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesUnauthenticatedWithFailureAutoRecoveryAesCtr256() throws IOException {
        canSkipBytesWithFailureAutoRecovery(AesCtrCipherDetails.INSTANCE_256_BIT, false);
    }

    public void canSkipBytesUnauthenticatedWithFailureAutoRecoveryAesGcm128() throws IOException {
        canSkipBytesWithFailureAutoRecovery(AesGcmCipherDetails.INSTANCE_128_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesUnauthenticatedWithFailureAutoRecoveryAesGcm192() throws IOException {
        canSkipBytesWithFailureAutoRecovery(AesGcmCipherDetails.INSTANCE_192_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesUnauthenticatedWithFailureAutoRecoveryAesGcm256() throws IOException {
        canSkipBytesWithFailureAutoRecovery(AesGcmCipherDetails.INSTANCE_256_BIT, false);
    }

    private void canDecryptEntireObjectAllReadModesWithFailureAutoRecovery(final SupportedCipherDetails cipherDetails,
                                                                           final boolean authenticate)
            throws IOException {
        System.out.printf("Testing %s decryption of [%s] as full read of stream with failure recovery%n",
                          authenticate ? "authenticated" : "unauthenticated",
                          cipherDetails.getCipherId());

        canDecryptEntireObject(cipherDetails, SingleReads.class, authenticate, 0, PRE_READ);
        canDecryptEntireObject(cipherDetails, SingleReads.class, authenticate, 0, POST_READ);
        canDecryptEntireObject(cipherDetails, ByteChunkReads.class, authenticate, 0, PRE_READ);
        canDecryptEntireObject(cipherDetails, ByteChunkReads.class, authenticate, 0, POST_READ);
        canDecryptEntireObject(cipherDetails, ByteChunkOffsetReads.class, authenticate, 0, PRE_READ);
        canDecryptEntireObject(cipherDetails, ByteChunkOffsetReads.class, authenticate, 0, POST_READ);

        // failure position doesn't matter for EOF
        // the extra EOF read does not occur when authentication is skipped and the cipher is not authenticated
        if (authenticate || cipherDetails.isAEADCipher()) {
            canDecryptEntireObject(cipherDetails, SingleReads.class, authenticate, -1, ON_EOF);
            canDecryptEntireObject(cipherDetails, ByteChunkReads.class, authenticate, -1, ON_EOF);
            canDecryptEntireObject(cipherDetails, ByteChunkOffsetReads.class, authenticate, -1, ON_EOF);
        }

        final Integer halfFileSize = Math.floorDiv(this.plaintextSize, 2);
        canDecryptEntireObject(cipherDetails, SingleReads.class, authenticate, halfFileSize, PRE_READ);
        canDecryptEntireObject(cipherDetails, SingleReads.class, authenticate, halfFileSize, POST_READ);
        canDecryptEntireObject(cipherDetails, ByteChunkReads.class, authenticate, halfFileSize, PRE_READ);
        canDecryptEntireObject(cipherDetails, ByteChunkReads.class, authenticate, halfFileSize, POST_READ);
        canDecryptEntireObject(cipherDetails, ByteChunkOffsetReads.class, authenticate, halfFileSize, PRE_READ);
        canDecryptEntireObject(cipherDetails, ByteChunkOffsetReads.class, authenticate, halfFileSize, POST_READ);
    }

    private void canSkipBytesWithFailureAutoRecovery(final SupportedCipherDetails cipherDetails,
                                                     final boolean authenticate) throws IOException {
        System.out.printf("Testing %s ciphertext with [%s] as read and skips of stream with %n",
                authenticate ? "authenticated" : "unauthenticated",
                cipherDetails.getCipherId());

        final Integer halfFileSize = Math.floorDiv(this.plaintextSize, 2);
        final Integer meoisBufferSize = MantaEncryptedObjectInputStream.calculateBufferSize((long) this.plaintextSize, cipherDetails);
        final Integer halfBufferSize = Math.floorDiv(meoisBufferSize, 2);

        // neither of these strategies reach the end of the file so ON_EOF does not make sense
        canReadObject(cipherDetails, ReadAndSkipPartialReadFirstHalfOfFile.class, false, 0, PRE_READ);
        canReadObject(cipherDetails, ReadAndSkipPartialReadFirstHalfOfFile.class, false, 0, POST_READ);
        canReadObject(cipherDetails, ReadAndSkipPartialReadFirstHalfOfFile.class, false, halfFileSize, PRE_READ);
        canReadObject(cipherDetails, ReadAndSkipPartialReadFirstHalfOfFile.class, false, halfFileSize, POST_READ);
        canReadObject(cipherDetails, ReadAndSkipPartialReadFirstHalfOfFile.class, false, meoisBufferSize - halfBufferSize, PRE_READ);
        canReadObject(cipherDetails, ReadAndSkipPartialReadFirstHalfOfFile.class, false, meoisBufferSize - halfBufferSize, POST_READ);
        canReadObject(cipherDetails, ReadAndSkipPartialReadFirstHalfOfFile.class, false, meoisBufferSize + halfBufferSize, PRE_READ);
        canReadObject(cipherDetails, ReadAndSkipPartialReadFirstHalfOfFile.class, false, meoisBufferSize + halfBufferSize, POST_READ);

        canReadObject(cipherDetails, ReadAndSkipPartialReadStaticSkipSize.class, false, 0, PRE_READ);
        canReadObject(cipherDetails, ReadAndSkipPartialReadStaticSkipSize.class, false, 0, POST_READ);
        canReadObject(cipherDetails, ReadAndSkipPartialReadStaticSkipSize.class, false, halfFileSize, PRE_READ);
        canReadObject(cipherDetails, ReadAndSkipPartialReadStaticSkipSize.class, false, halfFileSize, POST_READ);
        canReadObject(cipherDetails, ReadAndSkipPartialReadStaticSkipSize.class, false, meoisBufferSize - halfBufferSize, PRE_READ);
        canReadObject(cipherDetails, ReadAndSkipPartialReadStaticSkipSize.class, false, meoisBufferSize - halfBufferSize, POST_READ);
        canReadObject(cipherDetails, ReadAndSkipPartialReadStaticSkipSize.class, false, meoisBufferSize + halfBufferSize, PRE_READ);
        canReadObject(cipherDetails, ReadAndSkipPartialReadStaticSkipSize.class, false, meoisBufferSize + halfBufferSize, POST_READ);
    }

}
