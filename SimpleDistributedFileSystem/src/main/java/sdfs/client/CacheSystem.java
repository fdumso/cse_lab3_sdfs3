package sdfs.client;

import sdfs.datanode.DataNode;
import sdfs.exception.IllegalAccessTokenException;
import sdfs.filetree.BlockInfo;
import sdfs.filetree.FileNode;
import sdfs.filetree.LocatedBlock;

import java.util.UUID;

class CacheSystem {
    private UUID token;
    private FileNode fileNode;
    private int cacheSize;
    private CachedBlock[] cachedBlockList;
    private int pointer;

    CacheSystem(UUID token, FileNode fileNode, int cacheSize) {
        this.token = token;
        this.fileNode = fileNode;
        this.cacheSize = cacheSize;
        this.cachedBlockList = new CachedBlock[cacheSize];
        this.pointer = 0;
    }

    void flush() {
        for (CachedBlock cachedBlock :
                cachedBlockList) {
            if (cachedBlock != null && cachedBlock.dirty) {
                writeToServer(cachedBlock);
            }
        }
    }

    boolean isDirty(int blockNumber) {
        for (CachedBlock cachedBlock :
                cachedBlockList) {
            if (cachedBlock != null && blockNumber == cachedBlock.blockIndex) {
                return cachedBlock.dirty;
            }
        }
        return false;
    }

    void removeCachedBlock(int blockNumber) {
        for (int i = 0; i < cachedBlockList.length; i++) {
            CachedBlock cachedBlock = cachedBlockList[i];
            if (cachedBlock != null && blockNumber == cachedBlock.blockIndex) {
                remove(i);
            }
        }
    }

    byte[] read(int blockNumber) {
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

    void writeNew(int blockNumber, LocatedBlock locatedBlock, byte[] data) {
        for (CachedBlock cachedBlock :
                cachedBlockList) {
            if (cachedBlock != null && blockNumber == cachedBlock.blockIndex) {
                // set used
                touchBlock(cachedBlock, true);
                cachedBlock.data = data;
                cachedBlock.locatedBlock = locatedBlock;
                return;
            }
        }
        // not been cached, create a new cache
        addToCache(locatedBlock, blockNumber, data, true);
    }

    void write(int blockNumber, byte[] data) {
        for (CachedBlock cachedBlock :
                cachedBlockList) {
            if (cachedBlock != null && blockNumber == cachedBlock.blockIndex) {
                // set used
                touchBlock(cachedBlock, true);
                cachedBlock.data = data;
                return;
            }
        }
        // not been cached, read from server
        readFromServer(blockNumber);
        // try again
        write(blockNumber, data);
    }

    private void touchBlock(CachedBlock cachedBlock, boolean changed) {
        cachedBlock.flag = true;
        if (changed) {
            cachedBlock.dirty = true;
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
        cachedBlock.dirty = dirty;

        for (int i = 0; i < cachedBlockList.length; i++) {
            if (cachedBlockList[i] == null) {
                cachedBlockList[i] = cachedBlock;
                return;
            }
        }

        CachedBlock current = cachedBlockList[pointer];
        if (!current.flag) {
            remove(pointer);
        } else {
            current.flag = false;
        }
        next();

        addToCache(locatedBlock, blockNumber, data, dirty);
    }

    private void readFromServer(int blockNumber) throws IllegalArgumentException, IllegalAccessTokenException {
        BlockInfo blockInfo = fileNode.getBlockInfo(blockNumber);
        LocatedBlock locatedBlock = blockInfo.iterator().next();
        DataNodeStub dataNodeStub = new DataNodeStub(locatedBlock.getAddress(), locatedBlock.getPort());
        byte[] data = dataNodeStub.read(token, locatedBlock.getId(), 0, DataNode.BLOCK_SIZE);
        // add to cache
        addToCache(locatedBlock, blockNumber, data, false);
    }

    private void writeToServer(CachedBlock cachedBlock) {
        byte[] data = cachedBlock.data;
        LocatedBlock locatedBlock = cachedBlock.locatedBlock;
        DataNodeStub dataNodeStub = new DataNodeStub(locatedBlock.getAddress(), locatedBlock.getPort());
        dataNodeStub.write(token, locatedBlock.getId(), 0, data);
        cachedBlock.dirty = false;
    }


    class CachedBlock {
        private LocatedBlock locatedBlock;
        private int blockIndex;
        private byte[] data;
        private boolean flag;
        private boolean dirty;

        CachedBlock(LocatedBlock locatedBlock, int blockIndex, byte[] data) {
            this.locatedBlock = locatedBlock;
            this.data = data;
            this.blockIndex = blockIndex;
            this.flag = true;
            this.dirty = false;
        }
    }
}
