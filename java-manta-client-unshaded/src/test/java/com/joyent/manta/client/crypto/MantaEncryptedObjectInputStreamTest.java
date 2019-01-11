package com.joyent.manta.client.crypto;

import org.testng.annotations.Test;

import java.io.IOException;

@Test
public class MantaEncryptedObjectInputStreamTest extends AbstractMantaEncryptedObjectInputStreamTest {

    public void canDecryptEntireObjectAuthenticatedAesCbc128() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_128_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectAuthenticatedAesCbc192() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_192_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectAuthenticatedAesCbc256() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_256_BIT, true);
    }

    public void canDecryptEntireObjectAuthenticatedAesCtr128() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_128_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectAuthenticatedAesCtr192() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_192_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectAuthenticatedAesCtr256() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_256_BIT, true);
    }

    public void canDecryptEntireObjectAuthenticatedAesGcm128() throws IOException {
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_128_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectAuthenticatedAesGcm192() throws IOException {
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_192_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectAuthenticatedAesGcm256() throws IOException {
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_256_BIT, true);
    }


    public void canDecryptEntireObjectUnauthenticatedAesCbc128() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_128_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectUnauthenticatedAesCbc192() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_192_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectUnauthenticatedAesCbc256() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCbcCipherDetails.INSTANCE_256_BIT, false);
    }

    public void canDecryptEntireObjectUnauthenticatedAesCtr128() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_128_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectUnauthenticatedAesCtr192() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_192_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectUnauthenticatedAesCtr256() throws IOException {
        canDecryptEntireObjectAllReadModes(AesCtrCipherDetails.INSTANCE_256_BIT, false);
    }

    public void canDecryptEntireObjectUnauthenticatedAesGcm128() throws IOException {
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_128_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectUnauthenticatedAesGcm192() throws IOException {
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_192_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canDecryptEntireObjectUnauthenticatedAesGcm256() throws IOException {
        canDecryptEntireObjectAllReadModes(AesGcmCipherDetails.INSTANCE_256_BIT, false);
    }


    public void willErrorIfCiphertextIsModifiedAesCbc128() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesCbcCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAesCbc192() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesCbcCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAesCbc256() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesCbcCipherDetails.INSTANCE_256_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAesCtr128() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAesCtr192() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesCtrCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAesCtr256() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesCtrCipherDetails.INSTANCE_256_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAesGcm128() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesGcmCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAesGcm192() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesGcmCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAesGcm256() throws IOException {
        willErrorIfCiphertextIsModifiedAllReadModes(AesGcmCipherDetails.INSTANCE_256_BIT);
    }


    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesCbc128() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesCbcCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesCbc192() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesCbcCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesCbc256() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesCbcCipherDetails.INSTANCE_256_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesCtr128() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesCtr192() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesCtrCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesCtr256() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesCtrCipherDetails.INSTANCE_256_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesGcm128() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesGcmCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesGcm192() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesGcmCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAndNotReadFullyAesGcm256() throws IOException {
        willErrorIfCiphertextIsModifiedAndNotReadFully(AesGcmCipherDetails.INSTANCE_256_BIT);
    }


    public void canSkipBytesAuthenticatedAesCbc128() throws IOException {
        canSkipBytesAuthenticated(AesCbcCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesAuthenticatedAesCbc192() throws IOException {
        canSkipBytesAuthenticated(AesCbcCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesAuthenticatedAesCbc256() throws IOException {
        canSkipBytesAuthenticated(AesCbcCipherDetails.INSTANCE_256_BIT);
    }

    public void canSkipBytesAuthenticatedAesCtr128() throws IOException {
        canSkipBytesAuthenticated(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesAuthenticatedAesCtr192() throws IOException {
        canSkipBytesAuthenticated(AesCtrCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesAuthenticatedAesCtr256() throws IOException {
        canSkipBytesAuthenticated(AesCtrCipherDetails.INSTANCE_256_BIT);
    }

    public void canSkipBytesAuthenticatedAesGcm128() throws IOException {
        canSkipBytesAuthenticated(AesGcmCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesAuthenticatedAesGcm192() throws IOException {
        canSkipBytesAuthenticated(AesGcmCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesAuthenticatedAesGcm256() throws IOException {
        canSkipBytesAuthenticated(AesGcmCipherDetails.INSTANCE_256_BIT);
    }


    public void canSkipBytesUnauthenticatedAesCbc128() throws IOException {
        canSkipBytesUnauthenticated(AesCbcCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesUnauthenticatedAesCbc192() throws IOException {
        canSkipBytesUnauthenticated(AesCbcCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesUnauthenticatedAesCbc256() throws IOException {
        canSkipBytesUnauthenticated(AesCbcCipherDetails.INSTANCE_256_BIT);
    }

    public void canSkipBytesUnauthenticatedAesCtr128() throws IOException {
        canSkipBytesUnauthenticated(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesUnauthenticatedAesCtr192() throws IOException {
        canSkipBytesUnauthenticated(AesCtrCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesUnauthenticatedAesCtr256() throws IOException {
        canSkipBytesUnauthenticated(AesCtrCipherDetails.INSTANCE_256_BIT);
    }

    public void canSkipBytesUnauthenticatedAesGcm128() throws IOException {
        canSkipBytesUnauthenticated(AesGcmCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesUnauthenticatedAesGcm192() throws IOException {
        canSkipBytesUnauthenticated(AesGcmCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canSkipBytesUnauthenticatedAesGcm256() throws IOException {
        canSkipBytesUnauthenticated(AesGcmCipherDetails.INSTANCE_256_BIT);
    }


    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesCbc128() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesCbcCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesCbc192() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesCbcCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesCbc256() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesCbcCipherDetails.INSTANCE_256_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesCtr128() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesCtr192() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesCtrCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesCtr256() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesCtrCipherDetails.INSTANCE_256_BIT);
    }

    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesGcm128() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesGcmCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesGcm192() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesGcmCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsModifiedAndBytesAreSkippedAesGcm256() throws IOException {
        willErrorIfCiphertextIsModifiedAndBytesAreSkipped(AesGcmCipherDetails.INSTANCE_256_BIT);
    }

    public void canReadByteRangeStartingAtZeroEndingInFirstBlockAesCtr128() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() / 2;
        canReadByteRangeAllReadModes(cipherDetails, 0, endPos);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canReadByteRangeStartingAtZeroEndingInFirstBlockAesCtr192() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_192_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() / 2;
        canReadByteRangeAllReadModes(cipherDetails, 0, endPos);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canReadByteRangeStartingAtZeroEndingInFirstBlockAesCtr256() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_256_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() / 2;
        canReadByteRangeAllReadModes(cipherDetails, 0, endPos);
    }

    public void canReadByteRangeStartingAtZeroEndingInThirdBlockAesCtr128() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() * 2 + (cipherDetails.getBlockSizeInBytes() / 2);
        canReadByteRangeAllReadModes(cipherDetails, 0, endPos);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canReadByteRangeStartingAtZeroEndingInThirdBlockAesCtr192() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_192_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() * 2 + (cipherDetails.getBlockSizeInBytes() / 2);
        canReadByteRangeAllReadModes(cipherDetails, 0, endPos);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canReadByteRangeStartingAtZeroEndingInThirdBlockAesCtr256() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_256_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() * 2 + (cipherDetails.getBlockSizeInBytes() / 2);
        canReadByteRangeAllReadModes(cipherDetails, 0, endPos);
    }

    public void canReadByteRangeStartingAtThreeEndingInFirstBlockAesCtr128() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() / 2;
        canReadByteRangeAllReadModes(cipherDetails, 3, endPos);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canReadByteRangeStartingAtThreeEndingInFirstBlockAesCtr192() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_192_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() / 2;
        canReadByteRangeAllReadModes(cipherDetails, 3, endPos);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canReadByteRangeStartingAtThreeEndingInFirstBlockAesCtr256() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_256_BIT;
        int endPos = cipherDetails.getBlockSizeInBytes() / 2;
        canReadByteRangeAllReadModes(cipherDetails, 3, endPos);
    }

    public void canReadByteRangeStartingAtThirdBlockEndingInFifthBlockAesCtr128() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
        int startPos = cipherDetails.getBlockSizeInBytes() * 3 + (cipherDetails.getBlockSizeInBytes() / 2);
        int endPos = cipherDetails.getBlockSizeInBytes() * 5 + (cipherDetails.getBlockSizeInBytes() / 2);
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canReadByteRangeStartingAtThirdBlockEndingInFifthBlockAesCtr192() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_192_BIT;
        int startPos = cipherDetails.getBlockSizeInBytes() * 3 + (cipherDetails.getBlockSizeInBytes() / 2);
        int endPos = cipherDetails.getBlockSizeInBytes() * 5 + (cipherDetails.getBlockSizeInBytes() / 2);
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canReadByteRangeStartingAtThirdBlockEndingInFifthBlockAesCtr256() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_256_BIT;
        int startPos = cipherDetails.getBlockSizeInBytes() * 3 + (cipherDetails.getBlockSizeInBytes() / 2);
        int endPos = cipherDetails.getBlockSizeInBytes() * 5 + (cipherDetails.getBlockSizeInBytes() / 2);
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
    }

    public void canReadByteRangeStarting25bytesFromEndToEndAesCtr128() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
        int endPos = plaintextSize - 1;
        int startPos = endPos - 25;
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canReadByteRangeStarting25bytesFromEndToEndAesCtr192() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_192_BIT;
        int endPos = plaintextSize - 1;
        int startPos = endPos - 25;
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canReadByteRangeStarting25bytesFromEndToEndAesCtr256() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_256_BIT;
        int endPos = plaintextSize - 1;
        int startPos = endPos - 25;
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
    }

    public void canReadByteRangeStarting25bytesFromEndToBeyondEndAesCtr128() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
        int endPos = plaintextSize * 2;
        int startPos = plaintextSize - 26;
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canReadByteRangeStarting25bytesFromEndToBeyondEndAesCtr192() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_192_BIT;
        int endPos = plaintextSize * 2;
        int startPos = plaintextSize - 26;
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canReadByteRangeStarting25bytesFromEndToBeyondEndAesCtr256() throws IOException {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_256_BIT;
        int endPos = plaintextSize * 2;
        int startPos = plaintextSize - 26;
        canReadByteRangeAllReadModes(cipherDetails, startPos, endPos);
    }


    public void canCopyStreamWithLargeBufferBufferAuthenticatedAesCbc128() throws IOException {
         canCopyToOutputStreamWithLargeBuffer(AesCbcCipherDetails.INSTANCE_128_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canCopyStreamWithLargeBufferBufferAuthenticatedAesCbc192() throws IOException {
         canCopyToOutputStreamWithLargeBuffer(AesCbcCipherDetails.INSTANCE_192_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canCopyStreamWithLargeBufferBufferAuthenticatedAesCbc256() throws IOException {
         canCopyToOutputStreamWithLargeBuffer(AesCbcCipherDetails.INSTANCE_256_BIT, true);
    }

    public void canCopyStreamWithLargeBufferBufferAuthenticatedAesCtr128() throws IOException {
         canCopyToOutputStreamWithLargeBuffer(AesCtrCipherDetails.INSTANCE_128_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canCopyStreamWithLargeBufferBufferAuthenticatedAesCtr192() throws IOException {
         canCopyToOutputStreamWithLargeBuffer(AesCtrCipherDetails.INSTANCE_192_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canCopyStreamWithLargeBufferBufferAuthenticatedAesCtr256() throws IOException {
         canCopyToOutputStreamWithLargeBuffer(AesCtrCipherDetails.INSTANCE_256_BIT, true);
    }

    public void canCopyStreamWithLargeBufferBufferAuthenticatedAesGcm128() throws IOException {
         canCopyToOutputStreamWithLargeBuffer(AesGcmCipherDetails.INSTANCE_128_BIT, true);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canCopyStreamWithLargeBufferBufferAuthenticatedAesGcm192() throws IOException {
         canCopyToOutputStreamWithLargeBuffer(AesGcmCipherDetails.INSTANCE_192_BIT, true);
    }


    public void canCopyStreamWithLargeBufferBufferUnauthenticatedAesCbc128() throws IOException {
        canCopyToOutputStreamWithLargeBuffer(AesCbcCipherDetails.INSTANCE_128_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canCopyStreamWithLargeBufferBufferUnauthenticatedAesCbc192() throws IOException {
        canCopyToOutputStreamWithLargeBuffer(AesCbcCipherDetails.INSTANCE_192_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canCopyStreamWithLargeBufferBufferUnauthenticatedAesCbc256() throws IOException {
        canCopyToOutputStreamWithLargeBuffer(AesCbcCipherDetails.INSTANCE_256_BIT, false);
    }

    public void canCopyStreamWithLargeBufferBufferUnauthenticatedAesCtr128() throws IOException {
        canCopyToOutputStreamWithLargeBuffer(AesCtrCipherDetails.INSTANCE_128_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canCopyStreamWithLargeBufferBufferUnauthenticatedAesCtr192() throws IOException {
        canCopyToOutputStreamWithLargeBuffer(AesCtrCipherDetails.INSTANCE_192_BIT, false);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canCopyStreamWithLargeBufferBufferUnauthenticatedAesCtr256() throws IOException {
        canCopyToOutputStreamWithLargeBuffer(AesCtrCipherDetails.INSTANCE_256_BIT, false);
    }



    public void willErrorIfCiphertextIsMissingHmacAesCbc128() throws IOException {
        willErrorWhenMissingHMAC(AesCbcCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsMissingHmacAesCbc192() throws IOException {
        willErrorWhenMissingHMAC(AesCbcCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsMissingHmacAesCbc256() throws IOException {
        willErrorWhenMissingHMAC(AesCbcCipherDetails.INSTANCE_256_BIT);
    }

    public void willErrorIfCiphertextIsMissingHmacAesCtr128() throws IOException {
        willErrorWhenMissingHMAC(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsMissingHmacAesCtr192() throws IOException {
        willErrorWhenMissingHMAC(AesCtrCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willErrorIfCiphertextIsMissingHmacAesCtr256() throws IOException {
        willErrorWhenMissingHMAC(AesCtrCipherDetails.INSTANCE_256_BIT);
    }


    public void willValidateIfHmacIsReadInMultipleReadsAesCtr128() throws IOException {
        willValidateIfHmacIsReadInMultipleReads(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willValidateIfHmacIsReadInMultipleReadsAesCtr192() throws IOException {
        willValidateIfHmacIsReadInMultipleReads(AesCtrCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void willValidateIfHmacIsReadInMultipleReadsAesCtr256() throws IOException {
        willValidateIfHmacIsReadInMultipleReads(AesCtrCipherDetails.INSTANCE_256_BIT);
    }

}
