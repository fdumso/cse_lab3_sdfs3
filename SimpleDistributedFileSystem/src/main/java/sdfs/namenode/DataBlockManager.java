package sdfs.namenode;

import sdfs.filetree.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * to manage available block id of each data node
 * and record whether each block is still in use or not
 */
public class DataBlockManager {
    // map block id to its reference count
    // if the file that contains this block is in the current file tree
    // the reference count would be one
    // if a user is opening a file
    // then all the blocks of such file would +1 reference
    // if a user closes a file
    // then all the blocks of such file would -1 reference
    private ConcurrentMap<Integer, Integer> id2RefCount = new ConcurrentHashMap<>();

    DataBlockManager(DirNode root) {
        init(root);
    }

    private void init(DirNode node) {
        for (Entry e : node) {
            Node n = e.getNode();
            if (n.getType() == Node.Type.FILE) {
                for (BlockInfo blockInfo : ((FileNode) n)) {
                    for (LocatedBlock locatedBlock : blockInfo) {
                        int blockID = locatedBlock.getId();
                        id2RefCount.putIfAbsent(blockID, 1);
                    }
                }
            } else {
                init((DirNode) n);
            }
        }
    }

    public void addRef(FileNode fileNode) {

    }

    public int getNextBlockNumber() {
        int index = 0;
        while (true) {
            if (!id2RefCount.containsKey(index)) {
                return index;
            }
            index++;
        }
    }

}
