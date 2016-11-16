package sdfs.namenode;

import sdfs.exception.IllegalAccessTokenException;
import sdfs.protocol.INameNodeDataNodeProtocol;
import sdfs.protocol.INameNodeProtocol;

import java.io.IOException;
import java.nio.channels.OverlappingFileLockException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Created by pengcheng on 2016/11/15.
 */
public class NameNodeServer implements INameNodeProtocol, INameNodeDataNodeProtocol, Runnable {
    private long flushDiskInternalSeconds;

    public NameNodeServer(long flushDiskInternalSeconds) {
        this.flushDiskInternalSeconds = flushDiskInternalSeconds;
    }

    @Override
    public void run() {

    }

    @Override
    public AccessTokenPermission getAccessTokenOriginalPermission(UUID fileAccessToken) throws RemoteException {
        return null;
    }

    @Override
    public Set<Integer> getAccessTokenNewBlocks(UUID fileAccessToken) throws RemoteException {
        return null;
    }

    @Override
    public SDFSFileChannelData openReadonly(String fileUri) throws IOException {
        return null;
    }

    @Override
    public SDFSFileChannelData openReadwrite(String fileUri) throws OverlappingFileLockException, IOException {
        return null;
    }

    @Override
    public SDFSFileChannelData create(String fileUri) throws IOException {
        return null;
    }

    @Override
    public void closeReadonlyFile(UUID fileAccessToken) throws IllegalAccessTokenException, IOException {

    }

    @Override
    public void closeReadwriteFile(UUID fileAccessToken, long newFileSize) throws IllegalAccessTokenException, IllegalArgumentException, IOException {

    }

    @Override
    public void mkdir(String fileUri) throws IOException {

    }

    @Override
    public List<LocatedBlock> addBlocks(UUID fileAccessToken, int blockAmount) throws IllegalAccessTokenException, RemoteException {
        return null;
    }

    @Override
    public void removeLastBlocks(UUID fileAccessToken, int blockAmount) throws IllegalAccessTokenException, IndexOutOfBoundsException, RemoteException {

    }

    @Override
    public LocatedBlock newCopyOnWriteBlock(UUID fileAccessToken, int fileBlockNumber) throws IllegalStateException, RemoteException {
        return null;
    }
}
