package sdfs.filetree;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.*;

public class FileNode extends Node implements Serializable, Iterable<BlockInfo> {
    private static final long serialVersionUID = -5007570814999866661L;
    private List<BlockInfo> blockInfoList = new ArrayList<>();
    private long fileSize;

    public FileNode() {
        super(Type.FILE);
    }

    public void addBlockInfo(BlockInfo blockInfo) {
        blockInfoList.add(blockInfo);
    }

    public List<BlockInfo> getBlockInfoList() {
        return blockInfoList;
    }

    public BlockInfo getLastBlockInfo() {
        return blockInfoList.get(blockInfoList.size()-1);
    }

    public BlockInfo getBlockInfo(int i) {
        return blockInfoList.get(i);
    }

    public Set<Integer> getBlockNumberSetOfAddress(InetAddress inetAddress) {
        Set<Integer> result = new HashSet<>();
        for (BlockInfo blockInfo :
                blockInfoList) {
            for (LocatedBlock locatedBlock : blockInfo) {
                if (locatedBlock.getInetAddress().equals(inetAddress)) {
                    result.add(locatedBlock.getBlockNumber());
                }
            }
        }
        return result;
    }

    public void setBlockInfoByIndex( int index, BlockInfo blockInfo) {
        blockInfoList.set(index, blockInfo);
    }

    public void removeLastBlockInfo() {
        blockInfoList.remove(blockInfoList.size() - 1);
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public int getBlockAmount() {
        return blockInfoList.size();
    }

    @Override
    public Iterator<BlockInfo> iterator() {
        return blockInfoList.listIterator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        else return false;
    }

    public FileNode deepCopy() {
        FileNode fileNode = new FileNode();
        fileNode.setFileSize(this.fileSize);
        for (BlockInfo blockInfo :
                this.blockInfoList) {
            fileNode.addBlockInfo(blockInfo.deepCopy());
        }
        return fileNode;
    }

    public void assimilate(FileNode fileNode) {
        this.blockInfoList = fileNode.blockInfoList;
        this.fileSize = fileNode.fileSize;
    }

    @Override
    public int hashCode() {
        return blockInfoList.hashCode();
    }
}

