package sdfs.filetree;

import sdfs.entity.FileInfo;
import sdfs.namenode.DataBlockManager;
import sdfs.namenode.OpenedFileNode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileNode extends Node implements Serializable {
    private List<BlockInfo> blockInfoList = new ArrayList<>();
    private long fileSize;

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public FileNode() {
        super(Type.FILE);
    }

    @Override
    public void recordExistence(DataBlockManager dataBlockManager) {
        lock.readLock().lock();
        for (BlockInfo blockInfo : blockInfoList) {
            for (LocatedBlock locatedBlock : blockInfo) {
                dataBlockManager.recordExistence(locatedBlock);
            }
        }
        lock.readLock().unlock();
    }

    public void recordOpen(DataBlockManager dataBlockManager) {
        lock.readLock().lock();

        lock.readLock().unlock();
    }

    /**
     * open the file
     * deep copy this file node
     * use read lock to make sure file info would not be changed when copying it
     * @return the copy of the current file node
     */
    public OpenedFileNode open(DataBlockManager dataBlockManager) {
        lock.readLock().lock();
        // deep copy the file node
        List<BlockInfo> blockInfoList = new ArrayList<>();
        for (BlockInfo b : this.blockInfoList) {
            blockInfoList.add(b.copy());
        }
        FileInfo fileInfo = new FileInfo(blockInfoList, this.fileSize);
        OpenedFileNode openedFileNode = new OpenedFileNode(this, fileInfo);

        // record the openness of the file node
        dataBlockManager.recordOpen(blockInfoList);

        lock.readLock().unlock();
        return openedFileNode;
    }

    /**
     * close read write file
     * update old file node
     * notify DataBlockManager
     * use write lock to make sure it is atomic
     */
    public void closeUpdate(FileInfo fileInfo, DataBlockManager dataBlockManager) {
        lock.writeLock().lock();
        this.blockInfoList = fileInfo.getBlockInfoList();
        this.fileSize = fileInfo.getFileSize();
        dataBlockManager.recordClose(fileInfo.getBlockInfoList());
        lock.writeLock().unlock();
    }

    /**
     * close read write file
     * update old file node
     * notify DataBlockManager
     * use write lock to make sure it is atomic
     */
    public void close(FileInfo fileInfo, DataBlockManager dataBlockManager) {
        lock.writeLock().lock();
        dataBlockManager.recordClose(fileInfo.getBlockInfoList());
        lock.writeLock().unlock();
    }
}

