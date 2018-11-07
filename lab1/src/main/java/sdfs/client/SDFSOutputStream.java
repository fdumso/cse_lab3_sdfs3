/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.datanode.DataNode;
import sdfs.entity.FileNode;
import sdfs.entity.LocatedBlock;
import sdfs.namenode.NameNode;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

public class SDFSOutputStream implements Closeable, Flushable {
    private FileNode fileNode;
    private String fileUri;
    private int pos;

    public SDFSOutputStream(FileNode fileNode, String fileUri) {
        this.fileNode = fileNode;
        this.fileUri = fileUri;
        this.pos = 0;
    }

    public void write(byte[] b) throws IOException {
        if (fileNode == null) {
            throw new IOException();
        }
        int blockIndex = pos / DataNode.getBlockSize();
        int dataIndex = pos % DataNode.getBlockSize();
        int i = 0;
        while (i < b.length) {
            LocatedBlock block = fileNode.getBlock(blockIndex);
            if (block == null) {
                block = NameNode.getInstance().addBlock(fileUri);
            }
            int size = Math.min(b.length-i, DataNode.getBlockSize()-dataIndex);
            byte[] temp = new byte[size];
            for (int j = 0; j < size; j++) {
                temp[j] = b[i+j];
            }
            DataNode.getInstance().write(block.getBlockNumber(), dataIndex, size, temp);
            pos += size;
            i += size;
            dataIndex += size;
            if (dataIndex == DataNode.getBlockSize()) {
                dataIndex = 0;
                blockIndex++;
            }
        }
    }

    @Override
    public void flush() throws IOException {
        if (fileNode == null) {
            throw new IOException();
        }
    }

    @Override
    public void close() throws IOException {
        NameNode.getInstance().close(fileUri);
        fileNode = null;
    }
}
