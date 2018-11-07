package client;

import entity.SDFSFileChannel;

public class CachedBlockInfo {
    private SDFSFileChannel sdfsFileChannel;
    private int blockNumber;
    private boolean flag;
    private boolean changed;

    public CachedBlockInfo(SDFSFileChannel sdfsFileChannel, int blockNumber) {
        this.sdfsFileChannel = sdfsFileChannel;
        this.blockNumber = blockNumber;
        this.flag = true;
        this.changed = false;
    }

    public SDFSFileChannel getSdfsFileChannel() {
        return sdfsFileChannel;
    }

    public int getBlockNumber() {
        return blockNumber;
    }

    public boolean isOne() {
        return flag;
    }

    public boolean isChanged() {
        return changed;
    }

    public void setChanged() {
        changed = true;
    }

    public void setOne() {
        this.flag = true;
    }

    public void setZero() {
        this.flag = false;
    }

}
