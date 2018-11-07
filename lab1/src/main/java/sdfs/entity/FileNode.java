/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FileNode implements Serializable {
    private static final long serialVersionUID = -5007570814999866661L;
    private List<BlockInfo> blockInfoList;

    public FileNode() {
        this.blockInfoList = new ArrayList<>();
    }

    public int totalBlockNumber() {
        return blockInfoList.size();
    }

    public LocatedBlock getBlock(int index) {
        if (index < blockInfoList.size() && index >= 0) {
            return blockInfoList.get(index).getBlock();
        } else {
            return null;
        }
    }

    public void addBlock(LocatedBlock block) {
        BlockInfo blockInfo = new BlockInfo(block);
        blockInfoList.add(blockInfo);
    }

}

class BlockInfo implements Serializable {
    private static final long serialVersionUID = 8712105981933359634L;
    private List<LocatedBlock> locatedBlockList;

    public BlockInfo(LocatedBlock block) {
        this.locatedBlockList = new ArrayList<>();
        locatedBlockList.add(block);
    }

    public LocatedBlock getBlock() {
        return locatedBlockList.get(0);
    }
}