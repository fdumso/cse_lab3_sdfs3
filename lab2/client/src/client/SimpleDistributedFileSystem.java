/*
 * Copyright (c) Jipzingking 2016.
 */

package client;

import entity.SDFSFileChannel;

import java.io.IOException;
import java.net.InetSocketAddress;

public class SimpleDistributedFileSystem implements ISimpleDistributedFileSystem {
    private final NameNodeStub nameNodeStub;
    private int fileDataBlockCacheSize;

    /**
     * @param fileDataBlockCacheSize Buffer size for file data block. By default, it should be 16.
     *                               That means 16 block of data will be cache on local.
     *                               And you should use LRU algorithm to replace it.
     *                               It may change during test. So don't assert it will equal to a constant.
     */
    public SimpleDistributedFileSystem(InetSocketAddress nameNodeAddress, int fileDataBlockCacheSize) {
        this.nameNodeStub = new NameNodeStub(nameNodeAddress);
        this.fileDataBlockCacheSize = fileDataBlockCacheSize;
    }

    @Override
    public SDFSFileChannel openReadonly(String fileUri) throws IOException {
        SDFSFileChannel sdfsFileChannel = nameNodeStub.openReadonly(fileUri);
        CacheSystem cacheSystem = new CacheSystem(fileDataBlockCacheSize);
        sdfsFileChannel.setStuff(nameNodeStub, cacheSystem);
        return sdfsFileChannel;
    }

    @Override
    public SDFSFileChannel create(String fileUri) throws IOException {
        SDFSFileChannel sdfsFileChannel = nameNodeStub.create(fileUri);
        CacheSystem cacheSystem = new CacheSystem(fileDataBlockCacheSize);
        sdfsFileChannel.setStuff(nameNodeStub, cacheSystem);
        return sdfsFileChannel;
    }

    @Override
    public SDFSFileChannel openReadWrite(String fileUri) throws IOException {
        SDFSFileChannel sdfsFileChannel = nameNodeStub.openReadwrite(fileUri);
        CacheSystem cacheSystem = new CacheSystem(fileDataBlockCacheSize);
        sdfsFileChannel.setStuff(nameNodeStub, cacheSystem);
        return sdfsFileChannel;
    }

    @Override
    public void mkdir(String fileUri) throws IOException {
        nameNodeStub.mkdir(fileUri);
    }
}
