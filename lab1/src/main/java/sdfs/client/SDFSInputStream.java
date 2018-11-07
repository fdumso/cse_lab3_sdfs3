/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.datanode.DataNode;
import sdfs.entity.FileNode;
import sdfs.entity.LocatedBlock;

import java.io.Closeable;
import java.io.IOException;

public class SDFSInputStream implements Closeable {
    private FileNode fileNode;
    private String fileUri;
    private int pos;

    public SDFSInputStream(FileNode fileNode, String fileUri) throws IOException {
        this.fileNode = fileNode;
        this.fileUri = fileUri;
        this.pos = 0;
    }

    public int read(byte[] b) throws IOException {
        if (fileNode == null) {
            throw new IOException();
        }
        int blockIndex = pos / DataNode.getBlockSize();
        int dataIndex = pos % DataNode.getBlockSize();

        int i = 0;
        while (i < b.length) {
            LocatedBlock block = fileNode.getBlock(blockIndex);
            int size = Math.min(b.length-i, block.getSize()-dataIndex);
            if (size == 0) {
                break;
            }
            byte[] temp = new byte[size];
            DataNode.getInstance().read(block.getBlockNumber(), dataIndex, size, temp);
            for (int j = 0; j < size; j++) {
                b[i+j] = temp[j];
            }
            pos += size;
            i += size;
            if (dataIndex+size == DataNode.getBlockSize()) {
                dataIndex = 0;
                blockIndex++;
            } else {
                dataIndex += size;
            }
        }
        return i;
    }

    @Override
    public void close() throws IOException {
        fileNode = null;
    }

    public void seek(int newPos) throws IndexOutOfBoundsException, IOException {
        if (fileNode == null) {
            throw new IOException();
        }
        int totalPos = (fileNode.totalBlockNumber()-1) * DataNode.getBlockSize() + fileNode.getBlock(fileNode.totalBlockNumber()-1).getSize() - 1;
        if (newPos < totalPos && newPos >= 0) {
            this.pos = newPos;
        } else {
            throw new IndexOutOfBoundsException();
        }
    }
}
