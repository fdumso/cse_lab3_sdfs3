package sdfs.namenode;

import sdfs.exception.IllegalAccessTokenException;
import sdfs.exception.SDFSFileAlreadyExistsException;
import sdfs.filetree.*;
import sdfs.protocol.INameNodeDataNodeProtocol;
import sdfs.protocol.INameNodeProtocol;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.channels.OverlappingFileLockException;
import java.util.*;

import static sdfs.protocol.IDataNodeProtocol.BLOCK_SIZE;

public class NameNode implements INameNodeProtocol, INameNodeDataNodeProtocol {
    private static final String FILE_PATH = System.getProperty("sdfs.namenode.dir");

    private long flushDiskInternalSeconds;

    private final Map<UUID, FileNode> readonlyFile = new HashMap<>();
    private final Map<UUID, FileNode> readwriteFile = new HashMap<>();
    private final Map<UUID, FileNode> originalFile = new HashMap<>();
    private final Map<InetAddress, Map<Integer, Integer>> busyBlockCountMap = new HashMap<>();
    private DirNode rootNode;

    public NameNode(long flushDiskInternalSeconds) {
        this.flushDiskInternalSeconds = flushDiskInternalSeconds;

        // init file tree
        File rootDir = new File(FILE_PATH);
        if (!rootDir.exists()) {
            rootDir.mkdir();
        }
        // read root node
        File rootNodeFile = new File(FILE_PATH+"root.node");
        if (!rootNodeFile.exists()) {
            rootNode = new DirNode();
        } else {
            try {
                ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(rootNodeFile));
                rootNode = (DirNode) objectInputStream.readObject();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        // construct a table to store data node block number info
        initBlockListMap(rootNode);

        // check log
        // TODO
    }

    private void initBlockListMap(DirNode node) {
        for (Entry e : node) {
            Node n = e.getNode();
            if (n.getType() == Node.Type.FILE) {
                for (BlockInfo blockInfo : ((FileNode) n)) {
                    for (LocatedBlock locatedBlock : blockInfo) {
                        InetAddress address = locatedBlock.getInetAddress();
                        int blockNumber = locatedBlock.getBlockNumber();
                        Map<Integer, Integer> busyBlockCount;
                        if (busyBlockCountMap.containsKey(address)) {
                            busyBlockCount = busyBlockCountMap.get(address);
                        } else {
                            busyBlockCount = new HashMap<>();
                            busyBlockCountMap.put(address, busyBlockCount);
                        }
                        busyBlockCount.put(blockNumber, 1);
                    }
                }
            } else {
                initBlockListMap((DirNode) n);
            }
        }
    }

    private int getNextBlockNumber(InetAddress address) {
        if (busyBlockCountMap.containsKey(address)) {
            Map<Integer, Integer> busyBlockCount = busyBlockCountMap.get(address);
            int index = 0;
            while (true) {
                if (!busyBlockCount.containsKey(index)) {
                    busyBlockCount.put(index, 1);
                    return index;
                }
                index++;
            }
        } else {
            Map<Integer, Integer> busyBlockCount = new HashMap<>();
            busyBlockCountMap.put(address, busyBlockCount);
            busyBlockCount.put(0, 1);
            return 0;
        }
    }

//    private void updateNode() {
//        try {
//            FileOutputStream fileOutputStream = new FileOutputStream(FILE_PATH+"root.node");
//            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
//            objectOutputStream.writeObject(rootNode);
//            objectOutputStream.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

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

    @Override
    public AccessTokenPermission getAccessTokenPermission(UUID token) {
        FileNode fileNode;
        if ((fileNode = readwriteFile.get(token)) != null) {
            try {
                Set<Integer> allowedBlocks = fileNode.getBlockNumberSetOfAddress(InetAddress.getLocalHost());
                return new AccessTokenPermission(true, allowedBlocks);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        } else if ((fileNode = readonlyFile.get(token)) != null) {
            try {
                Set<Integer> allowedBlocks = fileNode.getBlockNumberSetOfAddress(InetAddress.getLocalHost());
                return new AccessTokenPermission(false, allowedBlocks);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public SDFSFileChannelData openReadonly(String fileUri) throws FileNotFoundException {
        FileNode fileNode = locateFile(fileUri);

        // copy the file node to keep the data unchanged
        FileNode readingNode = fileNode.deepCopy();
        UUID token = UUID.randomUUID();
        readonlyFile.put(token, readingNode);
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
        FileNode writingNode = fileNode.deepCopy();
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
            FileNode writingNode = fileNode.deepCopy();
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
        try {
            FileNode writingNode = readwriteFile.get(token);
            for (int i = 0; i < blockAmount; i++) {
                BlockInfo blockInfo = new BlockInfo();
                LocatedBlock locatedBlock = new LocatedBlock(InetAddress.getLocalHost(), getNextBlockNumber(InetAddress.getLocalHost()));
                blockInfo.addLocatedBlock(locatedBlock);
                writingNode.addBlockInfo(blockInfo);
                result.add(locatedBlock);
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
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
        try {
            locatedBlock = new LocatedBlock(InetAddress.getLocalHost(), getNextBlockNumber(InetAddress.getLocalHost()));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        blockInfo.addLocatedBlock(locatedBlock);
        writingNode.setBlockInfoByIndex(fileBlockNumber, blockInfo);
        return locatedBlock;
    }
}
