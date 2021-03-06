package sdfs.protocol;

import sdfs.entity.SDFSFileChannelData;
import sdfs.exception.IllegalAccessTokenException;
import sdfs.exception.SDFSFileAlreadyExistsException;
import sdfs.filetree.LocatedBlock;

import java.io.FileNotFoundException;
import java.nio.channels.OverlappingFileLockException;
import java.util.List;
import java.util.UUID;

public interface INameNodeProtocol {
    /**
     * Open a readonly file that is already exist.
     * Allow multi readonly access to the same file.
     * Also, if the file is currently being writing by other client, it is also LEGAL to open the same file. However, only after the write instance is closed could other client to read the new data.
     *
     * @param fileUri The file uri to be open
     * @return The SDFSFileChannelData represent the file
     * @throws FileNotFoundException if the file is not exist
     */
    SDFSFileChannelData openReadonly(String fileUri) throws FileNotFoundException;

    /**
     * Open a readwrite file that is already exist.
     * At most one UUID with readwrite permission could exist on the same file at the same time.
     *
     * @param fileUri The file uri to be open
     * @return The SDFSFileChannelData represent the file
     * @throws FileNotFoundException        if the file is not exist
     * @throws OverlappingFileLockException if the file is already opened readwrite
     */
    SDFSFileChannelData openReadwrite(String fileUri) throws OverlappingFileLockException, FileNotFoundException;

    /**
     * Create a empty file. It should maintain a readwrite file on the memory and return the accessToken to client.
     *
     * @param fileUri The file uri to be create
     * @return The SDFSFileChannelData represent the file.
     * @throws SDFSFileAlreadyExistsException if the file is already exist
     */
    SDFSFileChannelData create(String fileUri) throws SDFSFileAlreadyExistsException, FileNotFoundException;

    /**
     * Close a readonly file.
     *
     * @param fileAccessToken file to be closed
     * @throws IllegalAccessTokenException if accessToken is illegal
     */
    void closeReadonlyFile(UUID fileAccessToken) throws IllegalAccessTokenException;

    /**
     * Close a readwrite file. If file metadata has been changed, store them on the disk.
     *
     * @param fileAccessToken file to be closed
     * @param newFileSize     The new file size after modify
     * @throws IllegalArgumentException    if new file size not in (blockAmount * BLOCK_SIZE, (blockAmount + 1) * BLOCK_SIZE]
     * @throws IllegalAccessTokenException if accessToken is illegal
     */
    void closeReadwriteFile(UUID fileAccessToken, long newFileSize) throws IllegalAccessTokenException, IllegalArgumentException;

    /**
     * Make a directory on given file uri.
     *
     * @param fileUri the directory path
     * @throws SDFSFileAlreadyExistsException if directory or file is already exist
     */
    void mkdir(String fileUri) throws SDFSFileAlreadyExistsException, FileNotFoundException;

    /**
     * Request a special amount of free blocks for a file
     * No metadata should be written to disk until it is correctly close
     *
     * @param fileAccessToken the file accessToken with readwrite state
     * @param blockAmount     the request block amount
     * @return a special amount of blocks that is free and could be used by client
     * @throws IllegalAccessTokenException if access token is illegal
     */
    List<LocatedBlock> addBlocks(UUID fileAccessToken, int blockAmount) throws IllegalAccessTokenException;

    /**
     * Delete the last blocks for a file
     * No metadata should be written to disk until it is correctly close
     *
     * @param fileAccessToken the file accessToken with readwrite state
     * @param blockAmount     the blocks amount to be removed
     * @throws IllegalAccessTokenException if file is illegal
     * @throws IndexOutOfBoundsException   if there is no enough block in this file
     */
    void removeLastBlocks(UUID fileAccessToken, int blockAmount) throws IllegalAccessTokenException, IndexOutOfBoundsException;

    /**
     * Request a new open on write block in order to prevent destroy the original block
     *
     * @param fileBlockNumber the block number in the file that require open on write
     * @return a locatedBlock than could be used as open on write block
     * @throws IllegalStateException if there is already open on write on this file block
     */
    LocatedBlock newCopyOnWriteBlock(UUID fileAccessToken, int fileBlockNumber) throws IllegalAccessTokenException, IndexOutOfBoundsException;
}
