package sdfs.client;

import sdfs.exception.IllegalAccessTokenException;
import sdfs.filetree.BlockInfo;
import sdfs.filetree.FileNode;
import sdfs.filetree.LocatedBlock;
import sdfs.protocol.IDataNodeProtocol;

import java.util.UUID;

public class CacheSystem {
    private UUID token;
    private FileNode fileNode;
    private int cacheSize;
    private CachedBlock[] cachedBlockList;
    private int pointer;

    public CacheSystem(UUID token, FileNode fileNode, int cacheSize) {
        this.token = token;
        this.fileNode = fileNode;
        this.cacheSize = cacheSize;
        this.cachedBlockList = new CachedBlock[cacheSize];
        this.pointer = 0;
    }

    public void flush() {
        for (CachedBlock cachedBlock :
                cachedBlockList) {
            if (cachedBlock != null && cachedBlock.dirty) {
                writeToServer(cachedBlock);
            }
        }
    }

    public boolean isDirty(int blockNumber) {
        for (CachedBlock cachedBlock :
                cachedBlockList) {
            if (cachedBlock != null && blockNumber == cachedBlock.blockIndex) {
                return cachedBlock.dirty;
            }
        }
        return false;
    }

    public void removeCachedBlock(int blockNumber) {
        for (int i = 0; i < cachedBlockList.length; i++) {
            CachedBlock cachedBlock = cachedBlockList[i];
            if (cachedBlock != null && blockNumber == cachedBlock.blockIndex) {
                remove(i);
            }
        }
    }

    public byte[] read(int blockNumber) {
        // search in cache
        for (CachedBlock cachedBlock :
                cachedBlockList) {
            if (cachedBlock != null && blockNumber == cachedBlock.blockIndex) {
                // set used
                touchBlock(cachedBlock, false);
                return cachedBlock.data;
            }
        }
        // not been cached, read from server
        readFromServer(blockNumber);
        // try again
        return read(blockNumber);
    }

    public void writeNew(int blockNumber, LocatedBlock locatedBlock, byte[] data) {
        for (CachedBlock cachedBlock :
                cachedBlockList) {
            if (cachedBlock != null && blockNumber == cachedBlock.blockIndex) {
                // set used
                touchBlock(cachedBlock, true);
                cachedBlock.setData(data);
                cachedBlock.setLocatedBlock(locatedBlock);
                return;
            }
        }
        // not been cached, create a new cache
        addToCache(locatedBlock, blockNumber, data, true);
    }

    public void write(int blockNumber, byte[] data) {
        for (CachedBlock cachedBlock :
                cachedBlockList) {
            if (cachedBlock != null && blockNumber == cachedBlock.blockIndex) {
                // set used
                touchBlock(cachedBlock, true);
                cachedBlock.setData(data);
                return;
            }
        }
        // not been cached, read from server
        readFromServer(blockNumber);
        // try again
        write(blockNumber, data);
    }

    private void touchBlock(CachedBlock cachedBlock, boolean changed) {
        cachedBlock.setOne();
        if (changed) {
            cachedBlock.setDirty();
        }
    }

    private void remove(int index) {
        CachedBlock cachedBlock = cachedBlockList[index];
        if (cachedBlock.dirty) {
            writeToServer(cachedBlock);
        }
        cachedBlockList[index] = null;
    }

    private void next() {
        pointer = (pointer+1)%cacheSize;
    }

    private void addToCache(LocatedBlock locatedBlock, int blockNumber, byte[] data, boolean dirty) {
        CachedBlock cachedBlock = new CachedBlock(locatedBlock, blockNumber, data);
        if (dirty) {
            cachedBlock.setDirty();
        }

        for (int i = 0; i < cachedBlockList.length; i++) {
            if (cachedBlockList[i] == null) {
                cachedBlockList[i] = cachedBlock;
                return;
            }
        }

        CachedBlock current = cachedBlockList[pointer];
        if (!current.isOne()) {
            remove(pointer);
        } else {
            current.setZero();
        }
        next();

        addToCache(locatedBlock, blockNumber, data, dirty);
    }

    private void readFromServer(int blockNumber) throws IllegalArgumentException, IllegalAccessTokenException {
        BlockInfo blockInfo = fileNode.getBlockInfo(blockNumber);
        LocatedBlock locatedBlock = blockInfo.iterator().next();
        DataNodeStub dataNodeStub = new DataNodeStub(locatedBlock.getInetAddress());
        byte[] data = dataNodeStub.read(token, locatedBlock.getBlockNumber(), 0, IDataNodeProtocol.BLOCK_SIZE);
        // add to cache
        addToCache(locatedBlock, blockNumber, data, false);
    }

    private void writeToServer(CachedBlock cachedBlock) {
        byte[] data = cachedBlock.data;
        LocatedBlock locatedBlock = cachedBlock.locatedBlock;
        DataNodeStub dataNodeStub = new DataNodeStub(locatedBlock.getInetAddress());
        dataNodeStub.write(token, locatedBlock.getBlockNumber(), 0, data);
        cachedBlock.clean();
    }


    class CachedBlock {
        private LocatedBlock locatedBlock;
        private int blockIndex;
        private byte[] data;
        private boolean flag;
        private boolean dirty;

        public CachedBlock(LocatedBlock locatedBlock, int blockIndex, byte[] data) {
            this.locatedBlock = locatedBlock;
            this.data = data;
            this.blockIndex = blockIndex;
            this.flag = true;
            this.dirty = false;
        }

        public LocatedBlock getLocatedBlock() {
            return locatedBlock;
        }

        public int getBlockIndex() {
            return blockIndex;
        }

        public void setData(byte[] data) {
            this.data = data;
        }

        public void setLocatedBlock(LocatedBlock locatedBlock) {
            this.locatedBlock = locatedBlock;
        }

        public byte[] getData() {
            return data;
        }

        public boolean isOne() {
            return flag;
        }

        public boolean isDirty() {
            return dirty;
        }

        public void setDirty() {
            dirty = true;
        }

        public void setOne() {
            this.flag = true;
        }

        public void setZero() {
            this.flag = false;
        }

        public void clean() {
            dirty = false;
        }
    }
}
