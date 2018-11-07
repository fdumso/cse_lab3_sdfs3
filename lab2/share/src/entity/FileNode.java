/*
 * Copyright (c) Jipzingking 2016.
 */

package entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FileNode extends Node implements Serializable, Iterable<BlockInfo> {
    private static final long serialVersionUID = -5007570814999866661L;
    private final List<BlockInfo> blockInfoList = new ArrayList<>();
    private long fileSize; //file size should be checked when closing the file.

    public FileNode() {
        super(Type.FILE);
    }

    public void addBlockInfo(BlockInfo blockInfo) {
        blockInfoList.add(blockInfo);
    }

    public BlockInfo getLastBlockInfo() {
        return blockInfoList.get(blockInfoList.size()-1);
    }

    public BlockInfo getBlockInfo(int i) {
        return blockInfoList.get(i);
    }

    public void removeLastBlockInfo() {
        blockInfoList.remove(blockInfoList.size() - 1);
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(int fileSize) {
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
        if (o == null || getClass() != o.getClass()) return false;

        FileNode that = (FileNode) o;

        return blockInfoList.equals(that.blockInfoList);
    }

    @Override
    public int hashCode() {
        return blockInfoList.hashCode();
    }
}

