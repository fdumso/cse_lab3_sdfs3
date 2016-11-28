/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.entity.FileInfo;
import sdfs.entity.SDFSFileChannelData;
import sdfs.filetree.BlockInfo;
import sdfs.filetree.LocatedBlock;

import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.UUID;

import static sdfs.datanode.DataNode.BLOCK_SIZE;


public class SDFSFileChannel implements SeekableByteChannel, Flushable {
    // permission field
    private boolean writable;

    // data field
    private UUID token;
    private FileInfo fileInfo;

    // local field
    private long position;
    private boolean closed;


    private NameNodeStub nameNodeStub;
    private CacheSystem cacheSystem;

    SDFSFileChannel(SDFSFileChannelData data, NameNodeStub nameNodeStub, int fileDataBlockCacheSize) {
        this.writable = data.isWritable();

        this.token = data.getToken();
        this.fileInfo = data.getFileInfo();

        this.position = 0;
        this.closed = false;

        this.cacheSystem = new CacheSystem(token, fileInfo, fileDataBlockCacheSize);
        this.nameNodeStub = nameNodeStub;
    }

    public FileInfo getFileInfo() {
        return fileInfo;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (this.closed) {
            throw new ClosedChannelException();
        }
        if (position >= fileInfo.getFileSize()) {
            return -1;
        }
        long oldPos = position;

        while (dst.hasRemaining() && position < fileInfo.getFileSize()) {
            int blockIndex = (int) (position / BLOCK_SIZE);
            int offset = (int) (position % BLOCK_SIZE);

            int size = Math.min(dst.limit() - dst.position(), BLOCK_SIZE - offset);
            if (position + size > fileInfo.getFileSize()) {
                size = (int) (fileInfo.getFileSize() - position);
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

            if (blockIndex < fileInfo.getBlockAmount()) {
                // write on the block that may have data
                byte[] oldData = cacheSystem.read(blockIndex);
                byte[] newData = new byte[BLOCK_SIZE];
                System.arraycopy(oldData, 0, newData, 0, oldData.length);
                System.arraycopy(bytes, 0, newData, offset, size);
                // if the block has been cached and is dirty
                // we do not need to ask for a open on write block
                // instead, we can write on the local block
                if (cacheSystem.isDirty(blockIndex)) {
                    // write data to cache
                    cacheSystem.write(blockIndex, newData);
                } else {
                    // open on write
                    LocatedBlock cowBlock = nameNodeStub.newCopyOnWriteBlock(token, blockIndex);
                    BlockInfo blockInfo = new BlockInfo();
                    blockInfo.addLocatedBlock(cowBlock);
                    fileInfo.setBlockInfoByIndex(blockIndex, blockInfo);
                    // write data to cache
                    cacheSystem.writeNew(blockIndex, cowBlock, newData);
                }
            } else {
                // write on new block
                LocatedBlock newBlock = nameNodeStub.addBlocks(token, 1).get(0);
                BlockInfo blockInfo = new BlockInfo();
                blockInfo.addLocatedBlock(newBlock);
                fileInfo.addBlockInfo(blockInfo);
                cacheSystem.writeNew(blockIndex, newBlock, bytes);
            }
            // src.position(src.position()+size);
            position += size;
        }
        if (position > fileInfo.getFileSize()) {
            fileInfo.setFileSize(position);
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
        return fileInfo.getFileSize();
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
                if (newSize < fileInfo.getFileSize()) {
                    fileInfo.setFileSize(newSize);
                }
                if (position > newSize) {
                    position = newSize;
                }
                // if block number exceeded file size
                // remove redundant blocks, and clear cache
                int neededBlockAmount = (int) (fileInfo.getFileSize() / BLOCK_SIZE);
                if (fileInfo.getFileSize() % BLOCK_SIZE != 0) {
                    neededBlockAmount++;
                }
                if (neededBlockAmount < fileInfo.getBlockAmount()) {
                    int exceededBlockCount = 0;
                    for (int i = fileInfo.getBlockAmount(); i > neededBlockAmount; i--) {
                        // clear cache
                        cacheSystem.removeCachedBlock(i-1);
                        fileInfo.removeLastBlockInfo();
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
                nameNodeStub.closeReadwriteFile(token, fileInfo.getFileSize());
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
