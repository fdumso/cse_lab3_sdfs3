/*
 * Copyright (c) Jipzingking 2016.
 */

package entity;

import client.CacheSystem;
import client.DataNodeStub;
import client.NameNodeStub;
import protocol.IDataNodeProtocol;

import java.io.FileNotFoundException;
import java.io.Flushable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class SDFSFileChannel implements SeekableByteChannel, Flushable, Serializable {
    private static final long serialVersionUID = 6892411224902751501L;
    private final UUID uuid; //File uuid
    private long fileSize; //Size of this file
    private int blockAmount; //Total block amount of this file
    private final FileNode fileNode;
    private final boolean isReadOnly;
    private final Map<Integer, byte[]> dataBlocksCache = new HashMap<>(); //BlockNumber to DataBlock cache. byte[] or ByteBuffer are both acceptable.

    private long position;
    private boolean closed;
    private NameNodeStub nameNodeStub;
    private CacheSystem cacheSystem;

    public SDFSFileChannel(UUID uuid, long fileSize, int blockAmount, FileNode fileNode, boolean isReadOnly) {
        this.uuid = uuid;
        this.fileSize = fileSize;
        this.blockAmount = blockAmount;
        this.fileNode = fileNode;
        this.isReadOnly = isReadOnly;

        this.position = 0;
        this.closed = false;
    }

    public void setStuff(NameNodeStub nameNodeStub, CacheSystem cacheSystem) {
        this.nameNodeStub = nameNodeStub;
        this.cacheSystem = cacheSystem;
    }

    public void removeCache(int blockNumber, boolean changed) {
        if (changed) {
            writeToServer(blockNumber);
        }
        dataBlocksCache.remove(blockNumber);
    }

    private void writeToServer(int blockNumber) {
        byte[] data = dataBlocksCache.get(blockNumber);
        BlockInfo currentBlockInfo = fileNode.getBlockInfo(blockNumber);
        LocatedBlock currentLocatedBlock = currentBlockInfo.iterator().next();
        DataNodeStub dataNodeStub = new DataNodeStub(currentLocatedBlock.getInetAddress());
        dataNodeStub.write(uuid, currentLocatedBlock.getBlockNumber(), 0, data);
    }

    private void readFromServer(int blockNumber) {
        BlockInfo currentBlockInfo = fileNode.getBlockInfo(blockNumber);
        LocatedBlock currentLocatedBlock = currentBlockInfo.iterator().next();
        DataNodeStub dataNodeStub = new DataNodeStub(currentLocatedBlock.getInetAddress());
        byte[] data = new byte[IDataNodeProtocol.BLOCK_SIZE];
        try {
            data = dataNodeStub.read(uuid, currentLocatedBlock.getBlockNumber(), 0, IDataNodeProtocol.BLOCK_SIZE);
        } catch (FileNotFoundException ignored) {

        } finally {
            cacheSystem.cacheBlock(this, blockNumber);
            dataBlocksCache.put(blockNumber, data);
        }
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
        int totalSize = dst.array().length;
        int index = 0;
        while (index < totalSize) {
            int currentBlockInfoIndex = (int) (position / IDataNodeProtocol.BLOCK_SIZE);
            int offset = (int) (position % IDataNodeProtocol.BLOCK_SIZE);
            int size = Math.min(totalSize-index, IDataNodeProtocol.BLOCK_SIZE-offset);
            if (position + size > fileSize) {
                size = (int) (fileSize - position);
                index = totalSize-size;
            }
            if (!dataBlocksCache.containsKey(currentBlockInfoIndex)) {
                readFromServer(currentBlockInfoIndex);
            }
            byte[] data = dataBlocksCache.get(currentBlockInfoIndex);
            cacheSystem.useBlock(this, currentBlockInfoIndex, false);
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
        if (this.isReadOnly) {
            throw new NonWritableChannelException();
        }
        long oldPos = position;
        byte[] srcBytes = src.array();
        int totalSize = srcBytes.length;
        if (position + totalSize > blockAmount * IDataNodeProtocol.BLOCK_SIZE) {
            int exceededSize = (int) (position + totalSize - (blockAmount * IDataNodeProtocol.BLOCK_SIZE));
            int neededBlockAmount = exceededSize / IDataNodeProtocol.BLOCK_SIZE + 1;
            List<LocatedBlock> blockList = nameNodeStub.addBlocks(uuid, neededBlockAmount);
            for (LocatedBlock locatedBlock :
                    blockList) {
                BlockInfo blockInfo = new BlockInfo();
                blockInfo.addLocatedBlock(locatedBlock);
                fileNode.addBlockInfo(blockInfo);
                blockAmount++;
            }
        }
        int index = 0;
        while (index < totalSize) {
            int currentBlockInfoIndex = (int) (position / IDataNodeProtocol.BLOCK_SIZE);
            int offset = (int) (position % IDataNodeProtocol.BLOCK_SIZE);
            int size = Math.min(totalSize-index, IDataNodeProtocol.BLOCK_SIZE-offset);
            if (!dataBlocksCache.containsKey(currentBlockInfoIndex)) {
                readFromServer(currentBlockInfoIndex);
            }
            byte[] data = dataBlocksCache.get(currentBlockInfoIndex);
            for (int i = 0; i < size; i++) {
                data[i+offset] = srcBytes[i+index];
            }
            cacheSystem.useBlock(this, currentBlockInfoIndex, true);
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
    public SeekableByteChannel truncate(long size) throws IOException {
        if (this.closed) {
            throw new ClosedChannelException();
        }
        if (this.isReadOnly) {
            throw new NonWritableChannelException();
        }
        if (size < 0) {
            throw new IllegalArgumentException();
        } else {
            if (size < fileSize) {
                fileSize = size;
            }
            if (position > size) {
                position = size;
            }
            if (fileSize / IDataNodeProtocol.BLOCK_SIZE + 1 > blockAmount) {
                int exceededBlockAmount = (int) (fileSize / IDataNodeProtocol.BLOCK_SIZE) + 1 - blockAmount;
                for (int i = 0; i < exceededBlockAmount; i++) {
                    fileNode.removeLastBlockInfo();
                }
                nameNodeStub.removeLastBlocks(uuid, exceededBlockAmount);
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
        if (isReadOnly) {
            nameNodeStub.closeReadonlyFile(uuid);
        } else {
            nameNodeStub.closeReadwriteFile(uuid, (int) fileSize);
        }
    }

    @Override
    public void flush() throws IOException {
        for (Integer i :
                dataBlocksCache.keySet()) {
            if (cacheSystem.isChanged(this, i)) {
                writeToServer(i);
            }
        }
    }
}
