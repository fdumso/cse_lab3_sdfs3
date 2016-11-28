package sdfs.namenode;

import sdfs.entity.AccessTokenPermission;
import sdfs.entity.FileInfo;
import sdfs.entity.SDFSFileChannelData;
import sdfs.exception.IllegalAccessTokenException;
import sdfs.exception.SDFSFileAlreadyExistsException;
import sdfs.filetree.*;
import sdfs.protocol.INameNodeDataNodeProtocol;
import sdfs.protocol.INameNodeProtocol;
import sdfs.protocol.SDFSConfiguration;

import java.io.*;
import java.net.InetAddress;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;


public class NameNode implements INameNodeProtocol, INameNodeDataNodeProtocol {
    private static final String NAME_NODE_DIR = System.getProperty("sdfs.namenode.dir");
    private static final String FILE_TREE_PATH = NAME_NODE_DIR+"filetree.data";
    private static final String LOG_PATH = NAME_NODE_DIR+"namenode.log";
    private final SDFSConfiguration configuration;
    private final long flushDiskInternalSeconds;

    // components
    private final DataBlockManager dataBlockManager;
    private final OpenedFileNodeManager openedFileNodeManager;

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
        openedFileNodeManager = new OpenedFileNodeManager(dataBlockManager);
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
    we do not need to consider thread safety for this action
     */
    @Override
    public AccessTokenPermission getAccessTokenPermission(UUID token, InetAddress dataNodeAddress) {
        if (openedFileNodeManager.isReading(token)) {
            FileInfo fileInfo = openedFileNodeManager.getReadingFile(token).getFileInfo();
            Set<Integer> allowedBlocks = fileInfo.getBlockNumberSetOfAddress(dataNodeAddress);
            return new AccessTokenPermission(false, allowedBlocks);
        } else if (openedFileNodeManager.isWriting(token)) {
            FileInfo fileInfo = openedFileNodeManager.getWritingFile(token).getFileInfo();
            Set<Integer> allowedBlocks = fileInfo.getBlockNumberSetOfAddress(dataNodeAddress);
            return new AccessTokenPermission(true, allowedBlocks);
        }
        return null;
    }

    @Override
    public SDFSFileChannelData openReadonly(String fileUri) throws FileNotFoundException {
        FileNode fileNode = locateFile(fileUri);
        UUID token = UUID.randomUUID();

        // open the file node and record its openness
        OpenedFileNode readingNode = openedFileNodeManager.openRead(fileNode, token);

        return new SDFSFileChannelData(readingNode.getFileInfo(), false, token);
    }

    @Override
    public SDFSFileChannelData openReadwrite(String fileUri) throws OverlappingFileLockException, FileNotFoundException {
        FileNode fileNode = locateFile(fileUri);
        UUID token = UUID.randomUUID();

        OpenedFileNode writingNode = openedFileNodeManager.openWrite(fileNode, token);

        return new SDFSFileChannelData(writingNode.getFileInfo(), true, token);
    }

    @Override
    public SDFSFileChannelData create(String fileUri) throws SDFSFileAlreadyExistsException, FileNotFoundException {
        if (fileUri.endsWith("/")) {
            throw new FileNotFoundException();
        }
        String fileName = fileUri.substring(fileUri.lastIndexOf('/')+1);
        DirNode dirNode = locateDir(fileUri);

        UUID token = UUID.randomUUID();
        OpenedFileNode openedFileNode = dirNode.createFile(fileName, token, openedFileNodeManager);
        return new SDFSFileChannelData(openedFileNode.getFileInfo(), true, token);
    }

    @Override
    public void mkdir(String fileUri) throws SDFSFileAlreadyExistsException, FileNotFoundException {
        if (fileUri.endsWith("/")) {
            throw new FileNotFoundException();
        }
        String dirName = fileUri.substring(fileUri.lastIndexOf('/')+1);
        DirNode dirNode = locateDir(fileUri);

        dirNode.createDir(dirName);
    }

    @Override
    public void closeReadonlyFile(UUID token) throws IllegalAccessTokenException {
        openedFileNodeManager.closeRead(token);
    }

    @Override
    public void closeReadwriteFile(UUID token, long newFileSize) throws IllegalAccessTokenException, IllegalArgumentException {
        openedFileNodeManager.closeWrite(token, newFileSize);
    }

    /*
    Since token is unique to each client
    we do not need to consider thread safety for this action
     */
    @Override
    public List<LocatedBlock> addBlocks(UUID token, int blockAmount) throws IllegalAccessTokenException {
        List<LocatedBlock> newBlockList = new ArrayList<>();
        if (!openedFileNodeManager.isWriting(token)) {
            throw new IllegalAccessTokenException();
        }
        if (blockAmount < 0) {
            throw new IllegalArgumentException();
        }
        OpenedFileNode openedFileNode = openedFileNodeManager.getWritingFile(token);
        for (int i = 0; i < blockAmount; i++) {
            BlockInfo blockInfo = new BlockInfo();
            LocatedBlock locatedBlock = new LocatedBlock(configuration.getDataNodeAddress(), configuration.getDataNodePort(), dataBlockManager.getNextBlockNumber());
            blockInfo.addLocatedBlock(locatedBlock);
            openedFileNode.getFileInfo().addBlockInfo(blockInfo);
            newBlockList.add(locatedBlock);
        }
        dataBlockManager.recordOpen(newBlockList);
        return newBlockList;
    }

    /*
    Since token is unique to each client
    we do not need to consider thread safety for this action
     */
    @Override
    public void removeLastBlocks(UUID token, int blockAmount) throws IllegalAccessTokenException, IndexOutOfBoundsException {
        if (!openedFileNodeManager.isWriting(token)) {
            throw new IllegalAccessTokenException();
        }
        if (blockAmount < 0) {
            throw new IllegalArgumentException();
        }
        OpenedFileNode openedFileNode = openedFileNodeManager.getWritingFile(token);
        for (int i = 0; i < blockAmount; i++) {
            openedFileNode.getFileInfo().removeLastBlockInfo();
        }
    }

    /*
    Since token is unique to each client
    we do not need to consider thread safety for this action
     */
    @Override
    public LocatedBlock newCopyOnWriteBlock(UUID token, int fileBlockNumber) throws IllegalAccessTokenException, IndexOutOfBoundsException {
        if (!openedFileNodeManager.isWriting(token)) {
            throw new IllegalAccessTokenException();
        }
        OpenedFileNode openedFileNode = openedFileNodeManager.getWritingFile(token);
        if (fileBlockNumber < 0 || fileBlockNumber >= openedFileNode.getFileInfo().getBlockAmount()) {
            throw new IndexOutOfBoundsException();
        }
        BlockInfo blockInfo = new BlockInfo();
        LocatedBlock locatedBlock = null;
        locatedBlock = new LocatedBlock(configuration.getDataNodeAddress(), configuration.getDataNodePort(), dataBlockManager.getNextBlockNumber());
        blockInfo.addLocatedBlock(locatedBlock);
        openedFileNode.getFileInfo().setBlockInfoByIndex(fileBlockNumber, blockInfo);
        return locatedBlock;
    }
}
