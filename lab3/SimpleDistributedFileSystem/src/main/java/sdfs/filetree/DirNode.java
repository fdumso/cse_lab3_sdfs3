package sdfs.filetree;

import sdfs.exception.SDFSFileAlreadyExistsException;
import sdfs.namenode.DataBlockManager;
import sdfs.namenode.OpenedFileNode;
import sdfs.namenode.OpenedFileNodeManager;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DirNode extends Node implements Serializable {
    private final Set<Entry> entries = new HashSet<>();

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public DirNode() {
        super(Type.DIR);
    }

    @Override
    public void recordExistence(DataBlockManager dataBlockManager) {
        lock.readLock().lock();
        for (Entry entry : entries) {
            Node node = entry.getNode();
            node.recordExistence(dataBlockManager);
        }
        lock.readLock().unlock();
    }

    /**
     * find the entry with the name
     * use read lock to make sure it is atomic
     * @param name the name of the entry to find
     * @return the entry found, return null if not found
     */
    public Entry findEntry(String name) {
        lock.readLock().lock();
        for (Entry e : entries) {
            if (e.getName().equals(name)) {
                lock.readLock().unlock();
                return e;
            }
        }
        lock.readLock().unlock();
        return null;
    }


    /**
     * create a new file under this directory and open it
     * first check if there is already a entry with the same filename in this directory
     * use wite lock to make it atomic
     * @param fileName the file name of the newly created file
     * @return the file node created, return null if file already exists
     * @throws SDFSFileAlreadyExistsException if name already exists
     */
    public OpenedFileNode createFile(String fileName, UUID token, OpenedFileNodeManager openedFileNodeManager) throws SDFSFileAlreadyExistsException {
        lock.writeLock().lock();
        // check if there is already an entry with the same name in this directory
        // if there is, return null to acknowledge its caller
        for (Entry e : entries) {
            if (e.getName().equals(fileName)) {
                lock.writeLock().unlock();
                throw new SDFSFileAlreadyExistsException();
            }
        }
        // else create a new empty file node
        FileNode fileNode = new FileNode();
        // add it to this directory
        Entry newEntry = new Entry(fileName, fileNode);
        entries.add(newEntry);
        // open it
        OpenedFileNode writingNode = openedFileNodeManager.openWrite(fileNode, token);
        lock.writeLock().unlock();
        return writingNode;
    }

    /**
     * create a directory under this directory
     * @param dirName the dir name to be created
     * @throws SDFSFileAlreadyExistsException if name already exists
     */
    public void createDir(String dirName) throws SDFSFileAlreadyExistsException {
        lock.writeLock().lock();
        // check if there is already an entry with the same name in this directory
        // if there is, return null to acknowledge its caller
        for (Entry e : entries) {
            if (e.getName().equals(dirName)) {
                lock.writeLock().unlock();
                throw new SDFSFileAlreadyExistsException();
            }
        }
        // else create a new dir
        DirNode newDirNode = new DirNode();
        Entry newEntry = new Entry(dirName, newDirNode);
        entries.add(newEntry);
        lock.writeLock().unlock();
    }

    /**
     * override default write object method to lock the file tree
     * so that no change will made during flushing the disk
     * @param stream the output stream
     * @throws IOException io exception
     */
    private void writeObject(ObjectOutputStream stream) throws IOException {
        lock.readLock().lock();
        stream.defaultWriteObject();
        lock.readLock().unlock();
    }
}
