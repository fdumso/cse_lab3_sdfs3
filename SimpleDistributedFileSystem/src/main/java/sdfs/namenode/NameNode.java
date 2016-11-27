package sdfs.namenode;

import sdfs.exception.IllegalAccessTokenException;
import sdfs.exception.SDFSFileAlreadyExistsException;
import sdfs.filetree.*;
import sdfs.protocol.INameNodeDataNodeProtocol;
import sdfs.protocol.INameNodeProtocol;
import sdfs.protocol.SDFSConfiguration;

import java.io.*;
import java.net.InetAddress;
import java.nio.channels.OverlappingFileLockException;
import java.util.*;

import static sdfs.datanode.DataNode.BLOCK_SIZE;


public class NameNode implements INameNodeProtocol, INameNodeDataNodeProtocol {
    private static final String NAME_NODE_DIR = System.getProperty("sdfs.namenode.dir");
    private static final String FILE_TREE_PATH = NAME_NODE_DIR+"filetree.data";
    private static final String LOG_PATH = NAME_NODE_DIR+"namenode.log";
    private final SDFSConfiguration configuration;
    private final long flushDiskInternalSeconds;

    // components
    private final DataBlockManager dataBlockManager;
    private final OpeningFileManager openingFileManager;

    private DirNode rootNode;

    public NameNode(SDFSConfiguration configuration, long flushDiskInternalSeconds) {
        this.flushDiskInternalSeconds = flushDiskInternalSeconds;
        this.configuration = configuration;

        // read file tree stored on the disk
        File rootNodeFile = new File(FILE_TREE_PATH);
        if (!rootNodeFile.exists()) {
            rootNode = new DirNode();
        } else {
            try {
                ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(rootNodeFile));
                rootNode = (DirNode) objectInputStream.readObject();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

        // init components
        dataBlockManager = new DataBlockManager(rootNode);
        openingFileManager = new OpeningFileManager();
        DiskFlusher diskFlusher = new DiskFlusher(rootNode, LOG_PATH, flushDiskInternalSeconds);

        // start flushing to disk
        new Thread(diskFlusher).start();
    }

    /**
     * to located the directory of a file
     * since it does not modify the file tree, we do not need to make sure it is atomic
     * @param fileUri the uri that specify the file
     * @return the directory of the file specified by the uri
     * @throws FileNotFoundException if the directory does not exist
     */
    private DirNode locateDir(String fileUri) throws FileNotFoundException {
        String[] dirStrList = fileUri.split("/");
        int nextIndex = 0;
        DirNode currentRoot = rootNode;
        while (nextIndex < dirStrList.length-1) {
            Entry e = currentRoot.findEntry(dirStrList[nextIndex]);
            if (e == null || e.getNode().getType() == Node.Type.FILE) {
                throw new FileNotFoundException();
            }
            currentRoot = (DirNode) e.getNode();
            nextIndex++;
        }
        return currentRoot;
    }

    /**
     * to located the file specified by the uri
     * since it does not modify the file tree, we do not need to make sure it is atomic
     * @param fileUri the uri of the file to located
     * @return the file node
     * @throws FileNotFoundException if the file does not exist or the directory does not exist
     */
    private FileNode locateFile(String fileUri) throws FileNotFoundException {
        if (fileUri.endsWith("/")) {
            throw new FileNotFoundException();
        }
        String fileName = fileUri.substring(fileUri.lastIndexOf('/')+1);
        DirNode dirNode = locateDir(fileUri);
        Entry entry = dirNode.findEntry(fileName);
        if (entry == null || entry.getNode().getType() != Node.Type.FILE) {
            throw new FileNotFoundException();
        } else {
            return (FileNode) entry.getNode();
        }
    }

    /*
    Since token is unique to each client
    we do not need to consider thread safety for this transaction
     */
    @Override
    public AccessTokenPermission getAccessTokenPermission(UUID token, InetAddress dataNodeAddress) {
        if (openingFileManager.isReading(token)) {
            FileNode fileNode = openingFileManager.getReadingFile(token);
            Set<Integer> allowedBlocks = fileNode.getBlockNumberSetOfAddress(dataNodeAddress);
            return new AccessTokenPermission(true, allowedBlocks);
        } else if (openingFileManager.isWriting(token)) {
            FileNode fileNode = openingFileManager.getWritingFile(token);
            Set<Integer> allowedBlocks = fileNode.getBlockNumberSetOfAddress(dataNodeAddress);
            return new AccessTokenPermission(false, allowedBlocks);
        }
        return null;
    }

    @Override
    public SDFSFileChannelData openReadonly(String fileUri) throws FileNotFoundException {
        FileNode fileNode = locateFile(fileUri);
        UUID token = UUID.randomUUID();
        FileNode readingNode = openingFileManager.openRead(token, fileNode);

        return new SDFSFileChannelData(readingNode, false, token);
    }

    @Override
    public SDFSFileChannelData openReadwrite(String fileUri) throws OverlappingFileLockException, FileNotFoundException {
        FileNode fileNode = locateFile(fileUri);
        if (originalFile.containsValue(fileNode)) {
            throw new OverlappingFileLockException();
        }

        // copy the file node and sent it the client
        UUID token = UUID.randomUUID();
        FileNode writingNode = fileNode.copy();
        SDFSFileChannelData sdfsFileChannelData = new SDFSFileChannelData(writingNode, true, token);
        readwriteFile.put(token, writingNode);
        originalFile.put(token, fileNode);
        return sdfsFileChannelData;
    }

    @Override
    public SDFSFileChannelData create(String fileUri) throws SDFSFileAlreadyExistsException, FileNotFoundException {
        if (fileUri.endsWith("/")) {
            throw new FileNotFoundException();
        }
        String fileName = fileUri.substring(fileUri.lastIndexOf('/')+1);
        DirNode dirNode = locateDir(fileUri);

        Entry entry = dirNode.findEntry(fileName);
        if (entry != null) {
            throw new SDFSFileAlreadyExistsException();
        } else {
            // create a new empty file node
            FileNode fileNode = new FileNode();
            // add it to file tree
            Entry newEntry = new Entry(fileName, fileNode);
            dirNode.addEntry(newEntry);

            // copy the file node and sent it to client
            FileNode writingNode = fileNode.copy();
            UUID token = UUID.randomUUID();
            readwriteFile.put(token, writingNode);
            originalFile.put(token, fileNode);
            return new SDFSFileChannelData(writingNode, true, token);
        }
    }

    @Override
    public void mkdir(String fileUri) throws SDFSFileAlreadyExistsException, FileNotFoundException {
        if (fileUri.endsWith("/")) {
            throw new FileNotFoundException();
        }
        String dirName = fileUri.substring(fileUri.lastIndexOf('/')+1);
        DirNode dirNode = locateDir(fileUri);

        Entry entry = dirNode.findEntry(dirName);
        if (entry != null) {
            throw new SDFSFileAlreadyExistsException();
        } else {
            DirNode newDirNode = new DirNode();
            Entry newEntry = new Entry(dirName, newDirNode);
            dirNode.addEntry(newEntry);
        }
    }

    @Override
    public void closeReadonlyFile(UUID token) throws IllegalAccessTokenException {
        if (!readonlyFile.containsKey(token)) {
            throw new IllegalAccessTokenException();
        } else {
            // remove from reading node cache
            readonlyFile.remove(token);
        }
    }

    @Override
    public void closeReadwriteFile(UUID token, long newFileSize) throws IllegalAccessTokenException, IllegalArgumentException {
        if (!readwriteFile.containsKey(token)) {
            throw new IllegalAccessTokenException();
        }
        FileNode writingNode = readwriteFile.get(token);
        if (!originalFile.containsKey(token)) {
            throw new IllegalAccessTokenException();
        }
        FileNode oldFile = originalFile.get(token);
        // check file size
        int blockAmount = writingNode.getBlockAmount();
        if (newFileSize < 0 || newFileSize <= (blockAmount-1) * BLOCK_SIZE || newFileSize > blockAmount * BLOCK_SIZE) {
            // remove from cache
            readwriteFile.remove(token);
            originalFile.remove(token);
            throw new IllegalArgumentException();
        }
        // update file size
        if (writingNode.getFileSize() != newFileSize) {
            writingNode.setFileSize(newFileSize);
        }
        // replace old node in the file tree if the node has been changed
        if (!writingNode.equals(oldFile)) {
            oldFile.assimilate(writingNode);
        }
        // remove from cache
        readwriteFile.remove(token);
        originalFile.remove(token);
    }

    @Override
    public List<LocatedBlock> addBlocks(UUID token, int blockAmount) throws IllegalAccessTokenException {
        List<LocatedBlock> result = new ArrayList<>();
        if (!readwriteFile.containsKey(token)) {
            throw new IllegalAccessTokenException();
        }
        if (blockAmount < 0) {
            throw new IllegalArgumentException();
        }
        FileNode writingNode = readwriteFile.get(token);
        for (int i = 0; i < blockAmount; i++) {
            BlockInfo blockInfo = new BlockInfo();
            LocatedBlock locatedBlock = new LocatedBlock(configuration.getDataNodeAddress(), configuration.getDataNodePort(), getNextBlockNumber(configuration.getDataNodeAddress()));
            blockInfo.addLocatedBlock(locatedBlock);
            writingNode.addBlockInfo(blockInfo);
            result.add(locatedBlock);
        }
        return result;
    }

    @Override
    public void removeLastBlocks(UUID token, int blockAmount) throws IllegalAccessTokenException, IndexOutOfBoundsException {
        if (!readwriteFile.containsKey(token)) {
            throw new IllegalAccessTokenException();
        }
        if (blockAmount < 0) {
            throw new IllegalArgumentException();
        }
        FileNode writingNode = readwriteFile.get(token);
        for (int i = 0; i < blockAmount; i++) {
            writingNode.removeLastBlockInfo();
        }
    }

    @Override
    public LocatedBlock newCopyOnWriteBlock(UUID token, int fileBlockNumber) throws IllegalAccessTokenException, IndexOutOfBoundsException {
        if (!readwriteFile.containsKey(token)) {
            throw new IllegalAccessTokenException();
        }
        FileNode writingNode = readwriteFile.get(token);
        if (fileBlockNumber < 0 || fileBlockNumber >= writingNode.getBlockAmount()) {
            throw new IndexOutOfBoundsException();
        }
        BlockInfo blockInfo = new BlockInfo();
        LocatedBlock locatedBlock = null;
        locatedBlock = new LocatedBlock(configuration.getDataNodeAddress(), configuration.getDataNodePort(), getNextBlockNumber(configuration.getDataNodeAddress()));
        blockInfo.addLocatedBlock(locatedBlock);
        writingNode.setBlockInfoByIndex(fileBlockNumber, blockInfo);
        return locatedBlock;
    }
}
