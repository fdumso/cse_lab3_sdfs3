package sdfs.namenode;

import sdfs.exception.IllegalAccessTokenException;
import sdfs.exception.SDFSFileAlreadyExistsException;
import sdfs.protocol.INameNodeDataNodeProtocol;
import sdfs.protocol.INameNodeProtocol;

import java.io.FileNotFoundException;
import java.nio.channels.OverlappingFileLockException;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class NameNode implements INameNodeProtocol, INameNodeDataNodeProtocol {
    private long flushDiskInternalSeconds;

    public NameNode(long flushDiskInternalSeconds) {
        this.flushDiskInternalSeconds = flushDiskInternalSeconds;
    }

    @Override
    public AccessTokenPermission getAccessTokenOriginalPermission(UUID fileAccessToken) {
        return null;
    }

    @Override
    public Set<Integer> getAccessTokenNewBlocks(UUID fileAccessToken) {
        return null;
    }

    @Override
    public SDFSFileChannelData openReadonly(String fileUri) throws FileNotFoundException {
        return null;
    }

    @Override
    public SDFSFileChannelData openReadwrite(String fileUri) throws OverlappingFileLockException, FileNotFoundException {
        return null;
    }

    @Override
    public SDFSFileChannelData create(String fileUri) throws SDFSFileAlreadyExistsException {
        return null;
    }

    @Override
    public void closeReadonlyFile(UUID fileAccessToken) throws IllegalAccessTokenException {

    }

    @Override
    public void closeReadwriteFile(UUID fileAccessToken, long newFileSize) throws IllegalAccessTokenException, IllegalArgumentException {

    }

    @Override
    public void mkdir(String fileUri) throws SDFSFileAlreadyExistsException {

    }

    @Override
    public List<LocatedBlock> addBlocks(UUID fileAccessToken, int blockAmount) throws IllegalAccessTokenException {
        return null;
    }

    @Override
    public void removeLastBlocks(UUID fileAccessToken, int blockAmount) throws IllegalAccessTokenException, IndexOutOfBoundsException {

    }

    @Override
    public LocatedBlock newCopyOnWriteBlock(UUID fileAccessToken, int fileBlockNumber) throws IllegalStateException {
        return null;
    }
}
