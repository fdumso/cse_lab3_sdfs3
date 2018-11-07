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
    private final String NAME_NODE_DIR = System.getProperty("sdfs.namenode.dir");
    private final String FILE_TREE_PATH = NAME_NODE_DIR+"/root.node";
    private final String LOG_PATH = NAME_NODE_DIR+"/namenode.log";
    private final SDFSConfiguration configuration;

    // components
    private final DataBlockManager dataBlockManager;
    private final OpenedFileNodeManager openedFileNodeManager;
    private final Logger logger;

    private DirNode rootNode;

    public NameNode(SDFSConfiguration configuration, long flushDiskInternalSeconds) {
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
        logger = new Logger(LOG_PATH, this);
        DiskFlusher diskFlusher = new DiskFlusher(rootNode, logger, FILE_TREE_PATH, flushDiskInternalSeconds);

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
        // log START
        int logID = logger.start();

        try {
            UUID token = UUID.randomUUID();
            logger.openRead(logID, fileUri, token);
            FileNode fileNode = locateFile(fileUri);
            // open the file node and record its openness
            OpenedFileNode readingNode = openedFileNodeManager.openRead(fileNode, token);
            // ready to do
            logger.commit(logID);
            return new SDFSFileChannelData(readingNode.getFileInfo(), false, token);
        } catch (FileNotFoundException e) {
            // log ABORT
            logger.abort(logID);
            throw e;
        }
    }

    @Override
    public SDFSFileChannelData openReadwrite(String fileUri) throws OverlappingFileLockException, FileNotFoundException {
        // log START
        int logID = logger.start();

        try {
            UUID token = UUID.randomUUID();
            logger.openWrite(logID, fileUri, token);

            FileNode fileNode = locateFile(fileUri);
            OpenedFileNode writingNode = openedFileNodeManager.openWrite(fileNode, token);


            logger.commit(logID);
            return new SDFSFileChannelData(writingNode.getFileInfo(), true, token);
        } catch (OverlappingFileLockException | FileNotFoundException e) {
            // log ABORT
            logger.abort(logID);
            throw e;
        }

    }

    @Override
    public SDFSFileChannelData create(String fileUri) throws SDFSFileAlreadyExistsException, FileNotFoundException {
        // log START
        int logID = logger.start();

        try {
            UUID token = UUID.randomUUID();
            logger.create(logID, fileUri, token);

            if (fileUri.endsWith("/")) {
                throw new FileNotFoundException();
            }
            String fileName = fileUri.substring(fileUri.lastIndexOf('/')+1);
            DirNode dirNode = locateDir(fileUri);
            OpenedFileNode openedFileNode = dirNode.createFile(fileName, token, openedFileNodeManager);


            logger.commit(logID);
            return new SDFSFileChannelData(openedFileNode.getFileInfo(), true, token);
        } catch (SDFSFileAlreadyExistsException | FileNotFoundException e) {
            // log ABORT
            logger.abort(logID);
            throw e;
        }
    }

    @Override
    public void mkdir(String fileUri) throws SDFSFileAlreadyExistsException, FileNotFoundException {
        // log START
        int logID = logger.start();

        try {
            logger.mkdir(logID, fileUri);

            if (fileUri.endsWith("/")) {
                throw new FileNotFoundException();
            }
            String dirName = fileUri.substring(fileUri.lastIndexOf('/')+1);
            DirNode dirNode = locateDir(fileUri);
            dirNode.createDir(dirName);
            logger.commit(logID);

        } catch (SDFSFileAlreadyExistsException | FileNotFoundException e) {
            // log ABORT
            logger.abort(logID);
            throw e;
        }

    }

    @Override
    public void closeReadonlyFile(UUID token) throws IllegalAccessTokenException {
        // log START
        int logID = logger.start();

        try {
            logger.closeRead(logID, token);
            openedFileNodeManager.closeRead(token);
            logger.commit(logID);

        } catch (IllegalAccessTokenException e) {
            // log ABORT
            logger.abort(logID);
            throw e;
        }
    }

    @Override
    public void closeReadwriteFile(UUID token, long newFileSize) throws IllegalAccessTokenException, IllegalArgumentException {
        // log START
        int logID = logger.start();

        try {
            logger.closeWrite(logID, token, newFileSize);
            openedFileNodeManager.closeWrite(token, newFileSize);
            logger.commit(logID);

        } catch (IllegalAccessTokenException | IllegalArgumentException e) {
            // log ABORT
            logger.abort(logID);
            throw e;
        }
    }

    /*
    Since token is unique to each client
    we do not need to consider thread safety for this action
     */
    @Override
    public List<LocatedBlock> addBlocks(UUID token, int blockAmount) throws IllegalAccessTokenException, IllegalArgumentException {
        // log START
        int logID = logger.start();

        try {
            List<Integer> newBlockNumberList = new ArrayList<>();
            for (int i = 0; i < blockAmount; i++) {
                newBlockNumberList.add(dataBlockManager.getNextBlockNumber());
            }
            logger.addBlocks(logID, token, newBlockNumberList);
            if (!openedFileNodeManager.isWriting(token)) {
                throw new IllegalAccessTokenException();
            }
            if (blockAmount < 0) {
                throw new IllegalArgumentException();
            }
            List<LocatedBlock> newBlockList = new ArrayList<>();
            OpenedFileNode openedFileNode = openedFileNodeManager.getWritingFile(token);
            for (int i = 0; i < blockAmount; i++) {
                BlockInfo blockInfo = new BlockInfo();
                LocatedBlock locatedBlock = new LocatedBlock(configuration.getDataNodeAddress(), configuration.getDataNodePort(), newBlockNumberList.get(i));
                blockInfo.addLocatedBlock(locatedBlock);
                openedFileNode.getFileInfo().addBlockInfo(blockInfo);
                newBlockList.add(locatedBlock);
            }
            logger.commit(logID);
            return newBlockList;
        } catch (IllegalAccessTokenException | IllegalArgumentException e) {
            // log ABORT
            logger.abort(logID);
            throw e;
        }
    }

    /*
    Since token is unique to each client
    we do not need to consider thread safety for this action
     */
    @Override
    public void removeLastBlocks(UUID token, int blockAmount) throws IllegalAccessTokenException, IndexOutOfBoundsException {
        // log START
        int logID = logger.start();

        try {
            logger.removeBlocks(logID, token, blockAmount);
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
            logger.commit(logID);
        } catch (IllegalAccessTokenException | IndexOutOfBoundsException e) {
            // log ABORT
            logger.abort(logID);
            throw e;
        }
    }

    /*
    Since token is unique to each client
    we do not need to consider thread safety for this action
     */
    @Override
    public LocatedBlock newCopyOnWriteBlock(UUID token, int fileBlockNumber) throws IllegalAccessTokenException, IndexOutOfBoundsException {
        // log START
        int logID = logger.start();

        try {
            int newBlockNumber = dataBlockManager.getNextBlockNumber();
            logger.copyOnWriteBlock(logID, token, fileBlockNumber, newBlockNumber);
            if (!openedFileNodeManager.isWriting(token)) {
                throw new IllegalAccessTokenException();
            }
            OpenedFileNode openedFileNode = openedFileNodeManager.getWritingFile(token);
            if (fileBlockNumber < 0 || fileBlockNumber >= openedFileNode.getFileInfo().getBlockAmount()) {
                throw new IndexOutOfBoundsException();
            }
            BlockInfo blockInfo = new BlockInfo();
            LocatedBlock locatedBlock = new LocatedBlock(configuration.getDataNodeAddress(), configuration.getDataNodePort(), newBlockNumber);
            blockInfo.addLocatedBlock(locatedBlock);
            openedFileNode.getFileInfo().setBlockInfoByIndex(fileBlockNumber, blockInfo);
            logger.commit(logID);
            return locatedBlock;
        } catch (IllegalAccessTokenException | IndexOutOfBoundsException e) {
            // log ABORT
            logger.abort(logID);
            throw e;
        }
    }

    void redoOpenReadonly(String fileUri, UUID token) throws FileNotFoundException {
        FileNode fileNode = locateFile(fileUri);
        openedFileNodeManager.openRead(fileNode, token);
    }

    void redoOpenReadwrite(String fileUri, UUID token) throws FileNotFoundException, OverlappingFileLockException {
        FileNode fileNode = locateFile(fileUri);
        openedFileNodeManager.openWrite(fileNode, token);
    }

    void redoAddBlocks(UUID token, List<Integer> newBlockNumberList) {
        OpenedFileNode openedFileNode = openedFileNodeManager.getWritingFile(token);
        for (Integer aNewBlockNumberList : newBlockNumberList) {
            BlockInfo blockInfo = new BlockInfo();
            LocatedBlock locatedBlock = new LocatedBlock(configuration.getDataNodeAddress(), configuration.getDataNodePort(), aNewBlockNumberList);
            blockInfo.addLocatedBlock(locatedBlock);
            dataBlockManager.recordExistence(locatedBlock);
            openedFileNode.getFileInfo().addBlockInfo(blockInfo);
        }
    }

    void redoRemoveBlocks(UUID token, int blockAmount) {
        OpenedFileNode openedFileNode = openedFileNodeManager.getWritingFile(token);
        for (int i = 0; i < blockAmount; i++) {
            openedFileNode.getFileInfo().removeLastBlockInfo();
        }
    }

    void redoNewCopyOnWriteBlock(UUID token, int fileBlockNumber, int newBlockNumber) {
        OpenedFileNode openedFileNode = openedFileNodeManager.getWritingFile(token);
        BlockInfo blockInfo = new BlockInfo();
        LocatedBlock locatedBlock = new LocatedBlock(configuration.getDataNodeAddress(), configuration.getDataNodePort(), newBlockNumber);
        blockInfo.addLocatedBlock(locatedBlock);
        openedFileNode.getFileInfo().setBlockInfoByIndex(fileBlockNumber, blockInfo);
    }

    void redoCreate(String fileUri, UUID token) throws FileNotFoundException, SDFSFileAlreadyExistsException {
        String fileName = fileUri.substring(fileUri.lastIndexOf('/')+1);
        DirNode dirNode = locateDir(fileUri);
        dirNode.createFile(fileName, token, openedFileNodeManager);
    }

    void redoMkdir(String fileUri) throws SDFSFileAlreadyExistsException, FileNotFoundException {
        String dirName = fileUri.substring(fileUri.lastIndexOf('/')+1);
        DirNode dirNode = locateDir(fileUri);
        dirNode.createDir(dirName);
    }

    void redoCloseReadonly(UUID token) throws IllegalAccessTokenException {
        openedFileNodeManager.closeRead(token);
    }

    void redoCloseReadwrite(UUID token, long newFileSize) throws IllegalAccessTokenException, IllegalArgumentException {
        openedFileNodeManager.closeWrite(token, newFileSize);
    }
}
