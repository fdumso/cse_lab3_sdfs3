package sdfs.namenode;

import sdfs.exception.IllegalAccessTokenException;
import sdfs.filetree.FileNode;

import java.nio.channels.OverlappingFileLockException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static sdfs.datanode.DataNode.BLOCK_SIZE;

public class OpenedFileNodeManager {
    // map currently opened read-only file token to its accordingly opened file
    private final Map<UUID, OpenedFileNode> readingMap = new HashMap<>();
    // lock for writingMap cache
    private ReentrantReadWriteLock lockForReading = new ReentrantReadWriteLock();

    // map currently opened read-write file token to its accordingly opened file
    private final Map<UUID, OpenedFileNode> writingMap = new HashMap<>();
    // lock for writingMap cache
    private ReentrantReadWriteLock lockForWriting = new ReentrantReadWriteLock();

    private DataBlockManager dataBlockManager;

    OpenedFileNodeManager(DataBlockManager dataBlockManager) {
        this.dataBlockManager = dataBlockManager;
    }

    boolean isReading(UUID token) {
        lockForReading.readLock().lock();
        boolean isReading = readingMap.containsKey(token);
        lockForReading.readLock().unlock();
        return isReading;
    }

    boolean isWriting(UUID token) {
        lockForWriting.readLock().lock();
        boolean isWriting = writingMap.containsKey(token);
        lockForWriting.readLock().unlock();
        return isWriting;
    }

    OpenedFileNode getReadingFile(UUID token) {
        lockForReading.readLock().lock();
        OpenedFileNode readingNode = readingMap.get(token);
        lockForReading.readLock().unlock();
        return readingNode;
    }

    OpenedFileNode getWritingFile(UUID token) {
        lockForWriting.readLock().lock();
        OpenedFileNode writingNode = writingMap.get(token);
        lockForWriting.readLock().unlock();
        return writingNode;
    }

    OpenedFileNode openRead(FileNode fileNode, UUID token) {
        lockForReading.writeLock().lock();
        OpenedFileNode openedFileNode = fileNode.open(dataBlockManager);
        readingMap.put(token, openedFileNode);
        lockForReading.writeLock().unlock();
        return openedFileNode;
    }

    public OpenedFileNode openWrite(FileNode fileNode, UUID token) throws OverlappingFileLockException {
        lockForWriting.writeLock().lock();
        if (!writingMap.containsValue(new OpenedFileNode(fileNode, null))) {
            OpenedFileNode openedFileNode = fileNode.open(dataBlockManager);
            writingMap.put(token, openedFileNode);
            lockForWriting.writeLock().unlock();
            return openedFileNode;
        } else {
            lockForWriting.writeLock().unlock();
            throw new OverlappingFileLockException();
        }
    }

    void closeRead(UUID token) throws IllegalAccessTokenException {
        lockForReading.writeLock().lock();
        if (!readingMap.containsKey(token)) {
            lockForReading.writeLock().unlock();
            throw new IllegalAccessTokenException();
        } else {
            OpenedFileNode openedFileNode = readingMap.get(token);
            openedFileNode.getFileNode().close(openedFileNode.getFileInfo(), dataBlockManager);
            readingMap.remove(token);
            lockForReading.writeLock().unlock();
        }
    }

    void closeWrite(UUID token, long newFileSize) throws IllegalAccessTokenException, IllegalArgumentException {
        lockForWriting.writeLock().lock();
        if (!writingMap.containsKey(token)) {
            lockForWriting.writeLock().unlock();
            throw new IllegalAccessTokenException();
        } else {
            OpenedFileNode openedFileNode = writingMap.get(token);
            int blockAmount = openedFileNode.getFileInfo().getBlockAmount();
            if (newFileSize < 0 || newFileSize <= (blockAmount-1) * BLOCK_SIZE || newFileSize > blockAmount * BLOCK_SIZE) {
                // still need to remove from cache
                writingMap.remove(token);
                // but do not update file tree
                openedFileNode.getFileNode().close(openedFileNode.getFileInfo(), dataBlockManager);
                lockForWriting.writeLock().unlock();
                throw new IllegalArgumentException();
            } else {
                // remove from cache
                writingMap.remove(token);
                // first update file info
                openedFileNode.getFileInfo().setFileSize(newFileSize);
                // and update file tree
                openedFileNode.getFileNode().closeUpdate(openedFileNode.getFileInfo(), dataBlockManager);
                lockForWriting.writeLock().unlock();
            }
        }
    }
}
