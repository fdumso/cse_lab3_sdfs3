package sdfs.filetree;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class FileNode extends Node implements Serializable, Iterable<BlockInfo> {
    private static final long serialVersionUID = -5007570814999866661L;
    private AtomicReference<FileInfo> fileInfoAR = new AtomicReference<>();

    public FileNode() {
        super(Type.FILE);
        fileInfoAR.set(new FileInfo());
    }

    class FileInfo {
        private List<BlockInfo> blockInfoList = new ArrayList<>();
        private long fileSize;
    }

    public void addBlockInfo(BlockInfo blockInfo) {
        fileInfoAR.updateAndGet(fileInfo -> {
            fileInfo.blockInfoList.add(blockInfo);
            return fileInfo;
        });
    }

    public BlockInfo getBlockInfo(int i) {
        return fileInfoAR.get().blockInfoList.get(i);
    }

    public Set<Integer> getBlockNumberSetOfAddress(InetAddress inetAddress) {
        Set<Integer> result = new HashSet<>();
        for (BlockInfo blockInfo :
                fileInfoAR.get().blockInfoList) {
            for (LocatedBlock locatedBlock : blockInfo) {
                if (locatedBlock.getAddress().equals(inetAddress)) {
                    result.add(locatedBlock.getId());
                }
            }
        }
        return result;
    }

    public void setBlockInfoByIndex(int index, BlockInfo blockInfo) {
        fileInfoAR.updateAndGet(fileInfo -> {
            fileInfo.blockInfoList.set(index, blockInfo);
            return fileInfo;
        });
    }

    public void removeLastBlockInfo() {
        fileInfoAR.updateAndGet(fileInfo -> {
            fileInfo.blockInfoList.remove(fileInfo.blockInfoList.size()-1);
            return fileInfo;
        });
    }

    public long getFileSize() {
        return fileInfoAR.get().fileSize;
    }

    public void setFileSize(long fileSize) {
        fileInfoAR.updateAndGet(fileInfo -> {
            fileInfo.fileSize = fileSize;
            return fileInfo;
        });
    }

    public int getBlockAmount() {
        return fileInfoAR.get().blockInfoList.size();
    }

    @Override
    public Iterator<BlockInfo> iterator() {
        return fileInfoAR.get().blockInfoList.iterator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FileNode that = (FileNode) o;
        return fileInfoAR.get().blockInfoList.equals(that.fileInfoAR.get().blockInfoList);
    }

    /**
     * deep copy this file node
     * it is atomic
     * @return the copy
     */
    public FileNode copy() {
        FileNode fileNode = new FileNode();
        fileNode.fileInfoAR.updateAndGet(fileInfo -> {
            fileInfo.fileSize = this.getFileSize();
            for (BlockInfo b :
                    this) {
                fileInfo.blockInfoList.add(b.copy());
            }
            return fileInfo;
        });
        return fileNode;
    }

    @Override
    public int hashCode() {
        return fileInfoAR.get().blockInfoList.hashCode();
    }
}

