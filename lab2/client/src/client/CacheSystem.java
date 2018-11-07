package client;

import entity.SDFSFileChannel;

public class CacheSystem {
    private int cacheSize;
    private CachedBlockInfo[] cachedBlockList;
    private int pointer;

    public CacheSystem(int cacheSize) {
        this.cacheSize = cacheSize;
        cachedBlockList = new CachedBlockInfo[cacheSize];
        pointer = 0;
    }

    public void cacheBlock(SDFSFileChannel sdfsFileChannel, int blockNumber) {
        CachedBlockInfo cachedBlockInfo = new CachedBlockInfo(sdfsFileChannel, blockNumber);
        if (cachedBlockList[pointer] == null) {
            cachedBlockList[pointer] = cachedBlockInfo;
            next();
        } else {
            while (true) {
                CachedBlockInfo current = cachedBlockList[pointer];
                if (!current.isOne()) {
                    removeBlock(current);
                    cachedBlockList[pointer] = cachedBlockInfo;
                    next();
                    return;
                } else {
                    current.setZero();
                    next();
                }
            }
        }
    }

    private void next() {
        pointer = (pointer+1)%cacheSize;
    }

    public void useBlock(SDFSFileChannel sdfsFileChannel, int blockNumber, boolean changed) {
        for (CachedBlockInfo c :
                cachedBlockList) {
            if (c != null && c.getSdfsFileChannel() == sdfsFileChannel && c.getBlockNumber() == blockNumber) {
                c.setOne();
                if (changed) {
                    c.setChanged();
                }
            }
        }
    }

    public boolean isChanged(SDFSFileChannel sdfsFileChannel, int blockNumber) {
        for (CachedBlockInfo c :
                cachedBlockList) {
            if (c.getSdfsFileChannel() == sdfsFileChannel && c.getBlockNumber() == blockNumber) {
                return c.isChanged();
            }
        }
        return false;
    }

    private void removeBlock(CachedBlockInfo cachedBlockInfo) {
        cachedBlockInfo.getSdfsFileChannel().removeCache(cachedBlockInfo.getBlockNumber(), cachedBlockInfo.isChanged());
    }
}
