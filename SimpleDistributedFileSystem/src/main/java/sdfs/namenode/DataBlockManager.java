package sdfs.namenode;

import sdfs.filetree.BlockInfo;
import sdfs.filetree.DirNode;
import sdfs.filetree.LocatedBlock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * to manage available block id of each data node
 * and recordExistence whether each block is still in use or not
 */
public class DataBlockManager {
    // map block id to its reference count
    // if the file that contains this block is in the current file tree
    // the reference count would be one
    // if a user is opening a file
    // then all the blocks of such file would +1 reference
    // if a user closes a file
    // then all the blocks of such file would -1 reference
    private Map<Integer, Integer> id2RefCount = new HashMap<>();

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    DataBlockManager(DirNode root) {
        root.recordExistence(this);
    }

    public void recordExistence(LocatedBlock locatedBlock) {
        int blockID = locatedBlock.getId();
        lock.writeLock().lock();
        id2RefCount.putIfAbsent(blockID, 1);
        lock.writeLock().unlock();
    }

    public void recordOpen(Iterable<BlockInfo> blockInfoIterable) {
        lock.writeLock().lock();
        for (BlockInfo blockInfo : blockInfoIterable) {
            for (LocatedBlock locatedBlock : blockInfo) {
                int blockID = locatedBlock.getId();
                if (id2RefCount.containsKey(blockID)) {
                    int oldRefCount = id2RefCount.get(blockID);
                    id2RefCount.replace(blockID, oldRefCount+1);
                } else {
                    id2RefCount.put(blockID, 1);
                }
            }
        }
        lock.writeLock().unlock();
    }

    public void recordOpen(List<LocatedBlock> result) {
        lock.writeLock().lock();
        for (LocatedBlock locatedBlock : result) {
            int blockID = locatedBlock.getId();
            if (id2RefCount.containsKey(blockID)) {
                int oldRefCount = id2RefCount.get(blockID);
                id2RefCount.replace(blockID, oldRefCount+1);
            } else {
                id2RefCount.put(blockID, 1);
            }
        }
        lock.writeLock().unlock();
    }

    public void recordClose(Iterable<BlockInfo> blockInfoIterable) {
        lock.writeLock().lock();
        for (BlockInfo blockInfo : blockInfoIterable) {
            for (LocatedBlock locatedBlock : blockInfo) {
                int blockID = locatedBlock.getId();
                if (id2RefCount.containsKey(blockID)) {
                    int oldRefCount = id2RefCount.get(blockID);
                    if (oldRefCount == 1) {
                        id2RefCount.remove(blockID);
                    } else {
                        id2RefCount.replace(blockID, oldRefCount-1);
                    }
                }
            }
        }
        lock.writeLock().unlock();
    }

    public int getNextBlockNumber() {
        int index = 0;
        while (true) {
            if (!id2RefCount.containsKey(index) || id2RefCount.get(index) == 0) {
                return index;
            }
            index++;
        }
    }
}
