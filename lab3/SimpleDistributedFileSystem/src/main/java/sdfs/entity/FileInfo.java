package sdfs.entity;

import sdfs.filetree.BlockInfo;
import sdfs.filetree.LocatedBlock;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FileInfo implements Serializable {
    private List<BlockInfo> blockInfoList = new ArrayList<>();
    private long fileSize;

    public FileInfo(List<BlockInfo> blockInfoList, long fileSize) {
        this.blockInfoList = blockInfoList;
        this.fileSize = fileSize;
    }

    public List<BlockInfo> getBlockInfoList() {
        return blockInfoList;
    }

    public long getFileSize() {
        return fileSize;
    }

    public Set<Integer> getBlockNumberSetOfAddress(InetAddress inetAddress) {
        Set<Integer> result = new HashSet<>();
        for (BlockInfo blockInfo : blockInfoList) {
            for (LocatedBlock locatedBlock : blockInfo) {
                if (locatedBlock.getAddress().equals(inetAddress)) {
                    result.add(locatedBlock.getId());
                }
            }
        }
        return result;
    }

    public BlockInfo getBlockInfo(int blockNumber) {
        return blockInfoList.get(blockNumber);
    }

    public int getBlockAmount() {
        return blockInfoList.size();
    }

    public void setBlockInfoByIndex(int blockIndex, BlockInfo blockInfo) {
        blockInfoList.set(blockIndex, blockInfo);
    }

    public void addBlockInfo(BlockInfo blockInfo) {
        blockInfoList.add(blockInfo);
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public void removeLastBlockInfo() {
        blockInfoList.remove(blockInfoList.size()-1);
    }
}
