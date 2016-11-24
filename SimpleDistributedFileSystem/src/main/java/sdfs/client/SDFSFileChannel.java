/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.filetree.BlockInfo;
import sdfs.filetree.FileNode;
import sdfs.filetree.LocatedBlock;
import sdfs.namenode.SDFSFileChannelData;

import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.UUID;

import static sdfs.protocol.IDataNodeProtocol.BLOCK_SIZE;

public class SDFSFileChannel implements SeekableByteChannel, Flushable {
    private static final long serialVersionUID = 6892411224902751501L;

    // permission field
    private boolean writable;

    // data field
    private UUID token;
    private FileNode fileNode;

    // local field
    private long position;
    private boolean closed;


    private NameNodeStub nameNodeStub;
    private CacheSystem cacheSystem;

    public SDFSFileChannel(SDFSFileChannelData data, NameNodeStub nameNodeStub, int fileDataBlockCacheSize) {
        this.writable = data.isWritable();

        this.token = data.getToken();
        this.fileNode = data.getFileNode();

        this.position = 0;
        this.closed = false;

        this.cacheSystem = new CacheSystem(token, fileNode, fileDataBlockCacheSize);
        this.nameNodeStub = nameNodeStub;
    }

    public FileNode getFileNode() {
        return fileNode;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (this.closed) {
            throw new ClosedChannelException();
        }
        if (position >= fileNode.getFileSize()) {
            return -1;
        }
        long oldPos = position;

        while (dst.hasRemaining() && position < fileNode.getFileSize()) {
            int blockIndex = (int) (position / BLOCK_SIZE);
            int offset = (int) (position % BLOCK_SIZE);

            int size = Math.min(dst.limit() - dst.position(), BLOCK_SIZE - offset);
            if (position + size > fileNode.getFileSize()) {
                size = (int) (fileNode.getFileSize() - position);
            }
            byte[] data = cacheSystem.read(blockIndex);
            dst.put(data, offset, size);
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

        // write data
        while (src.hasRemaining()) {
            int blockIndex = (int) (position / BLOCK_SIZE);
            int offset = (int) (position % BLOCK_SIZE);
            int size = Math.min(src.limit() - src.position(), BLOCK_SIZE - offset);
            byte[] bytes = new byte[size];
            src = src.get(bytes);

            if (blockIndex < fileNode.getBlockAmount()) {
                // write on the block that may have data
                byte[] oldData = cacheSystem.read(blockIndex);
                byte[] newData = new byte[BLOCK_SIZE];
                for (int i = 0; i < oldData.length; i++) {
                    newData[i] = oldData[i];
                }
                for (int i = 0; i < size; i++) {
                    newData[offset+i] = bytes[i];
                }
                // if the block has been cached and is dirty
                // we do not need to ask for a copy on write block
                // instead, we can write on the local block
                if (cacheSystem.isDirty(blockIndex)) {
                    // write data to cache
                    cacheSystem.write(blockIndex, newData);
                } else {
                    // copy on write
                    LocatedBlock cowBlock = nameNodeStub.newCopyOnWriteBlock(token, blockIndex);
                    BlockInfo blockInfo = new BlockInfo();
                    blockInfo.addLocatedBlock(cowBlock);
                    fileNode.setBlockInfoByIndex(blockIndex, blockInfo);
                    // write data to cache
                    cacheSystem.writeNew(blockIndex, cowBlock, newData);
                }
            } else {
                // write on new block
                LocatedBlock newBlock = nameNodeStub.addBlocks(token, 1).get(0);
                BlockInfo blockInfo = new BlockInfo();
                blockInfo.addLocatedBlock(newBlock);
                fileNode.addBlockInfo(blockInfo);
                cacheSystem.writeNew(blockIndex, newBlock, bytes);
            }
            // src.position(src.position()+size);
            position += size;
        }
        if (position > fileNode.getFileSize()) {
            fileNode.setFileSize(position);
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
        return fileNode.getFileSize();
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
                if (newSize < fileNode.getFileSize()) {
                    fileNode.setFileSize(newSize);
                }
                if (position > newSize) {
                    position = newSize;
                }
                // if block number exceeded file size
                // remove redundant blocks, and clear cache
                int neededBlockAmount = (int) (fileNode.getFileSize() / BLOCK_SIZE);
                if (fileNode.getFileSize() % BLOCK_SIZE != 0) {
                    neededBlockAmount++;
                }
                if (neededBlockAmount < fileNode.getBlockAmount()) {
                    int exceededBlockCount = 0;
                    for (int i = fileNode.getBlockAmount(); i > neededBlockAmount; i--) {
                        // clear cache
                        cacheSystem.removeCachedBlock(i-1);
                        fileNode.removeLastBlockInfo();
                        exceededBlockCount++;
                    }
                    nameNodeStub.removeLastBlocks(token, exceededBlockCount);
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
        if (!closed) {
            cacheSystem.flush();
            this.closed = true;
            if (writable) {
                nameNodeStub.closeReadwriteFile(token, fileNode.getFileSize());
            } else {
                nameNodeStub.closeReadonlyFile(token);
            }
        }
    }

    @Override
    public void flush() throws IOException {
        if (this.closed) {
            throw new ClosedChannelException();
        }
        cacheSystem.flush();
    }
}
