/*
 * Copyright (c) Jipzingking 2016.
 */

package namenode;

import entity.*;
import protocol.IDataNodeProtocol;
import protocol.INameNodeDataNodeProtocol;
import protocol.INameNodeProtocol;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.FileAlreadyExistsException;
import java.util.*;

public class NameNode implements INameNodeProtocol, INameNodeDataNodeProtocol {

    private static final String FILE_PATH = "namenode/res/";
    private final Map<UUID, FileNode> readonlyFile = new HashMap<>();
    private final Map<UUID, FileNode> readwritePFile = new HashMap<>();
    private DirNode rootNode;
    private final Map<InetAddress, ArrayList<Integer>> busyBlockListMap = new HashMap<>();

    public NameNode() {
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
    }

    private void initBlockListMap(DirNode node) {
        Iterator<Entry> entryIterator = node.iterator();
        while (entryIterator.hasNext()) {
            Entry e = entryIterator.next();
            Node n = e.getNode();
            if (n.getType() == Node.Type.FILE) {
                Iterator<BlockInfo> blockInfoIterator = ((FileNode) n).iterator();
                while (blockInfoIterator.hasNext()) {
                    BlockInfo blockInfo = blockInfoIterator.next();
                    Iterator<LocatedBlock> locatedBlockIterator = blockInfo.iterator();
                    while (locatedBlockIterator.hasNext()) {
                        LocatedBlock locatedBlock = locatedBlockIterator.next();
                        InetAddress address = locatedBlock.getInetAddress();
                        int blockNumber = locatedBlock.getBlockNumber();
                        ArrayList<Integer> busyBlockList;
                        if (busyBlockListMap.containsKey(address)) {
                            busyBlockList = busyBlockListMap.get(address);
                        } else {
                            busyBlockList = new ArrayList<>();
                            busyBlockListMap.put(address, busyBlockList);
                        }
                        busyBlockList.add(blockNumber);
                    }
                }
            } else {
                initBlockListMap((DirNode) n);
            }
        }
    }

    private int getNextBlockNumber(InetAddress address) {
        if (busyBlockListMap.containsKey(address)) {
            ArrayList<Integer> busyBlockList = busyBlockListMap.get(address);
            int index = 0;
            while (true) {
                if (!busyBlockList.contains(index)) {
                    busyBlockList.add(index);
                    return index;
                }
                index++;
            }
        } else {
            ArrayList<Integer> busyBlockList = new ArrayList<>();
            busyBlockListMap.put(address, busyBlockList);
            busyBlockList.add(0);
            return 0;
        }
    }

    private void updateNode() {
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(FILE_PATH+"root.node");
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
            objectOutputStream.writeObject(rootNode);
            objectOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private DirNode locateDir(String dirUri) throws FileNotFoundException {
        String[] dirStrList = dirUri.split("/");
        int nextIndex = 1;
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
    public SDFSFileChannel openReadonly(String fileUri) throws FileNotFoundException {
        FileNode fileNode = locateFile(fileUri);
        UUID uuid = UUID.randomUUID();
        SDFSFileChannel sdfsFileChannel = new SDFSFileChannel(uuid, fileNode.getFileSize(), fileNode.getBlockAmount(), fileNode, true);
        readonlyFile.put(uuid, fileNode);
        return sdfsFileChannel;
    }

    @Override
    public SDFSFileChannel openReadwrite(String fileUri) throws FileNotFoundException, IllegalStateException {
        FileNode fileNode = locateFile(fileUri);
        if (readwritePFile.containsValue(fileNode)) {
            throw new IllegalStateException();
        }
        UUID uuid = UUID.randomUUID();
        SDFSFileChannel sdfsFileChannel = new SDFSFileChannel(uuid, fileNode.getFileSize(), fileNode.getBlockAmount(), fileNode, false);
        readwritePFile.put(uuid, fileNode);
        return sdfsFileChannel;
    }

    @Override
    public SDFSFileChannel create(String fileUri) throws FileAlreadyExistsException {
        if (fileUri.endsWith("/")) {
            throw new FileAlreadyExistsException(fileUri);
        }
        String fileName = fileUri.substring(fileUri.lastIndexOf('/')+1);
        String[] dirStrList = fileUri.split("/");
        int nextIndex = 1;
        DirNode currentRoot = rootNode;
        Entry currentEntry;
        while (nextIndex < dirStrList.length-1) {
            currentEntry = currentRoot.findEntry(dirStrList[nextIndex]);
            if (currentEntry == null) {
                String newDirUri = fileUri.substring(0, fileUri.lastIndexOf('/')+1);
                mkdir(newDirUri);
                continue;
            } else if (currentEntry.getNode().getType() == Node.Type.FILE) {
                throw new FileAlreadyExistsException(fileUri);
            }
            currentRoot = (DirNode) currentEntry.getNode();
            nextIndex++;
        }
        if (currentRoot.findEntry(fileName) != null) {
            throw new FileAlreadyExistsException(fileUri);
        } else {
            FileNode fileNode = new FileNode();
            Entry newEntry = new Entry(fileName, fileNode);
            currentRoot.addEntry(newEntry);
            UUID uuid = UUID.randomUUID();
            readwritePFile.put(uuid, fileNode);
            SDFSFileChannel sdfsFileChannel = new SDFSFileChannel(uuid, 0, 0, fileNode, false);
            return sdfsFileChannel;
        }
    }

    @Override
    public void closeReadonlyFile(UUID fileUuid) throws IllegalStateException {
        if (!readonlyFile.containsKey(fileUuid)) {
            throw new IllegalStateException();
        } else {
            readonlyFile.remove(fileUuid);
        }
    }

    @Override
    public void closeReadwriteFile(UUID fileUuid, int newFileSize) throws IllegalStateException, IllegalArgumentException {
        if (!readwritePFile.containsKey(fileUuid)) {
            throw new IllegalStateException();
        }
        FileNode fileNode = readwritePFile.get(fileUuid);
        int blockAmount = fileNode.getBlockAmount();
        if (newFileSize < (blockAmount-1) * IDataNodeProtocol.BLOCK_SIZE || newFileSize > blockAmount * IDataNodeProtocol.BLOCK_SIZE) {
            throw new IllegalArgumentException();
        }
        readwritePFile.remove(fileUuid);
        if (fileNode.getFileSize() != newFileSize) {
            fileNode.setFileSize(newFileSize);
            updateNode();
        }
    }

    @Override
    public void mkdir(String fileUri) throws FileAlreadyExistsException {
        if (!fileUri.endsWith("/")) {
            throw new IllegalArgumentException(fileUri);
        }
        String[] dirStrList = fileUri.split("/");
        int nextIndex = 1;
        DirNode currentRoot = rootNode;
        while (nextIndex < dirStrList.length-1) {
            Entry e = currentRoot.findEntry(dirStrList[nextIndex]);
            if (e == null) {
                DirNode dirNode = new DirNode();
                e = new Entry(dirStrList[nextIndex], dirNode);
                currentRoot.addEntry(e);
            }
            currentRoot = (DirNode) e.getNode();
            nextIndex++;
        }
        Entry e = currentRoot.findEntry(dirStrList[dirStrList.length-1]);
        if (e != null) {
            throw new FileAlreadyExistsException(fileUri);
        } else {
            DirNode dirNode = new DirNode();
            e = new Entry(dirStrList[dirStrList.length-1], dirNode);
            currentRoot.addEntry(e);
        }
    }

    @Override
    public LocatedBlock addBlock(UUID fileUuid) throws IllegalStateException {
        if (!readwritePFile.containsKey(fileUuid)) {
            throw new IllegalStateException();
        }
        try {
            FileNode fileNode = readwritePFile.get(fileUuid);
            BlockInfo blockInfo = new BlockInfo();
            LocatedBlock locatedBlock = new LocatedBlock(InetAddress.getLocalHost(), getNextBlockNumber(InetAddress.getLocalHost()));
            blockInfo.addLocatedBlock(locatedBlock);
            fileNode.addBlockInfo(blockInfo);
            return locatedBlock;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<LocatedBlock> addBlocks(UUID fileUuid, int blockAmount) throws IllegalStateException {
        List<LocatedBlock> result = new ArrayList<>();
        if (!readwritePFile.containsKey(fileUuid)) {
            throw new IllegalStateException();
        }
        try {
            FileNode fileNode = readwritePFile.get(fileUuid);
            for (int i = 0; i < blockAmount; i++) {
                BlockInfo blockInfo = new BlockInfo();
                LocatedBlock locatedBlock = new LocatedBlock(InetAddress.getLocalHost(), getNextBlockNumber(InetAddress.getLocalHost()));
                blockInfo.addLocatedBlock(locatedBlock);
                fileNode.addBlockInfo(blockInfo);
                result.add(locatedBlock);
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public void removeLastBlock(UUID fileUuid) throws IllegalStateException {
        if (!readwritePFile.containsKey(fileUuid)) {
            throw new IllegalStateException();
        }
        FileNode fileNode = readwritePFile.get(fileUuid);
        BlockInfo blockInfo = fileNode.getLastBlockInfo();
        Iterator<LocatedBlock> locatedBlockIterator = blockInfo.iterator();
        while (locatedBlockIterator.hasNext()) {
            LocatedBlock locatedBlock = locatedBlockIterator.next();
            busyBlockListMap.get(locatedBlock.getInetAddress()).remove(new Integer(locatedBlock.getBlockNumber()));
            blockInfo.removeLocatedBlock(locatedBlock);
        }
        fileNode.removeLastBlockInfo();
    }

    @Override
    public void removeLastBlocks(UUID fileUuid, int blockAmount) throws IllegalStateException {
        if (!readwritePFile.containsKey(fileUuid)) {
            throw new IllegalStateException();
        }
        FileNode fileNode = readwritePFile.get(fileUuid);
        for (int i = 0; i < blockAmount; i++) {
            BlockInfo blockInfo = fileNode.getLastBlockInfo();
            Iterator<LocatedBlock> locatedBlockIterator = blockInfo.iterator();
            while (locatedBlockIterator.hasNext()) {
                LocatedBlock locatedBlock = locatedBlockIterator.next();
                busyBlockListMap.get(locatedBlock.getInetAddress()).remove(new Integer(locatedBlock.getBlockNumber()));
                blockInfo.removeLocatedBlock(locatedBlock);
            }
            fileNode.removeLastBlockInfo();
        }
    }
}
