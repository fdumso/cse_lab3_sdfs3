/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.exception.IllegalAccessTokenException;
import sdfs.filetree.FileNode;
import sdfs.filetree.LocatedBlock;
import sdfs.namenode.SDFSFileChannelData;
import sdfs.protocol.IDataNodeProtocol;

import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.Set;
import java.util.UUID;

public class SDFSFileChannel implements SeekableByteChannel, Flushable {
    private static final long serialVersionUID = 6892411224902751501L;
    private static final int BLOCK_SIZE = IDataNodeProtocol.BLOCK_SIZE;

    // permission field
    private boolean writable;
    private Set<Integer> allowedBlocks;

    // data field
    private UUID token;
    private FileNode fileNode;
    private long fileSize; //Size of this file
    private int blockAmount; //Total block amount of this file

    // local field
    private long position;
    private boolean closed;


    private NameNodeStub nameNodeStub;
    private CacheSystem cacheSystem;

    public SDFSFileChannel(SDFSFileChannelData data, NameNodeStub nameNodeStub, int fileDataBlockCacheSize) {
        this.writable = data.getPermission().isWritable();
        this.allowedBlocks = data.getPermission().getAllowedBlocks();

        this.token = data.getToken();
        this.fileNode = data.getFileNode();
        this.fileSize = data.getFileSize();
        this.blockAmount = data.getBlockAmount();

        this.position = 0;
        this.closed = false;

        this.cacheSystem = new CacheSystem(token, fileNode, fileDataBlockCacheSize);
        this.nameNodeStub = nameNodeStub;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (this.closed) {
            throw new ClosedChannelException();
        }
        if (position >= fileSize) {
            return -1;
        }
        long oldPos = position;
        int readSize = dst.array().length;
        int index = 0;
        while (index < readSize) {
            int blockIndex = (int) (position / BLOCK_SIZE);
            int offset = (int) (position % BLOCK_SIZE);

            int size = Math.min(readSize - index, BLOCK_SIZE - offset);
            if (position + size > fileSize) {
                size = (int) (fileSize - position);
                index = readSize - size;
            }
            if (!allowedBlocks.contains(blockIndex)) {
                throw new IllegalAccessTokenException();
            }
            byte[] data = cacheSystem.read(blockIndex);
            dst.put(data, offset, size);
            index += size;
            position += size;
        }
        return (int) (position - oldPos);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if (this.closed) {
            throw new ClosedChannelException();
        }
        if (!this.writable) {
            throw new NonWritableChannelException();
        }
        long oldPos = position;
        byte[] srcBytes = src.array();
        int totalSize = srcBytes.length;

        // write data
        int index = 0;
        while (index < totalSize) {
            int blockIndex = (int) (position / BLOCK_SIZE);
            int offset = (int) (position % BLOCK_SIZE);
            int size = Math.min(totalSize - index, BLOCK_SIZE - offset);

            if (blockIndex < blockAmount) {
                // write on the block that may have data

                // if the block has been cached and is dirty
                // we do not need to ask for a copy on write block
                // instead, we can write on the local block
                if (!cacheSystem.isDirty(blockIndex)) {
                    // copy on write
                    if (!allowedBlocks.contains(blockIndex)) {
                        throw new IllegalAccessTokenException();
                    }
                }
                byte[] oldData = cacheSystem.read(blockIndex);
                LocatedBlock cowBlock = nameNodeStub.newCopyOnWriteBlock(token, blockIndex);
                // update the data
                for (int i = 0; i < size; i++) {
                    oldData[i+offset] = srcBytes[i+index];
                }
                // write data to cache
                cacheSystem.write(blockIndex, cowBlock, oldData);
            } else {
                // write on new block
                LocatedBlock newBlock = nameNodeStub.addBlocks(token, 1).get(0);
                byte[] data = new byte[size];
                for (int i = 0; i < size; i++) {
                    data[i] = srcBytes[i+index];
                }
                cacheSystem.write(blockIndex, newBlock, data);
            }
            index += size;
            position += size;
        }
        if (position > fileSize) {
            fileSize = position;
        }
        return (int) (position - oldPos);
    }

    @Override
    public long position() throws IOException {
        if (this.closed) {
            throw new ClosedChannelException();
        }
        return position;
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        if (this.closed) {
            throw new ClosedChannelException();
        }
        if (newPosition < 0) {
            throw new IllegalArgumentException();
        } else {
            this.position = newPosition;
        }
        return this;
    }

    @Override
    public long size() throws IOException {
        if (this.closed) {
            throw new ClosedChannelException();
        }
        return fileSize;
    }

    @Override
    public SeekableByteChannel truncate(long newSize) throws IOException {
        if (this.closed) {
            throw new ClosedChannelException();
        }
        if (!this.writable) {
            throw new NonWritableChannelException();
        } else {
            if (newSize < 0) {
                throw new IllegalArgumentException();
            } else {
                if (newSize < fileSize) {
                    fileSize = newSize;
                }
                if (position > newSize) {
                    position = newSize;
                }
                // if block number exceeded file size
                // remove redundant blocks
                if (fileSize / IDataNodeProtocol.BLOCK_SIZE + 1 > blockAmount) {
                    int exceededBlockAmount = (int) (fileSize / IDataNodeProtocol.BLOCK_SIZE) + 1 - blockAmount;
                    for (int i = 0; i < exceededBlockAmount; i++) {
                        fileNode.removeLastBlockInfo();
                    }
                    nameNodeStub.removeLastBlocks(token, exceededBlockAmount);
                }
            }
        }
        return this;
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    @Override
    public void close() throws IOException {
        flush();
        this.closed = true;
        if (writable) {
            nameNodeStub.closeReadwriteFile(token, fileSize);
        } else {
            nameNodeStub.closeReadonlyFile(token);
        }
    }

    @Override
    public void flush() throws IOException {
        //todo your code here
    }
}
