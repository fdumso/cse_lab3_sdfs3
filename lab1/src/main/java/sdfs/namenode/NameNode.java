package sdfs.namenode;

import sdfs.datanode.DataNode;
import sdfs.entity.FileNode;
import sdfs.entity.LocatedBlock;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.HashMap;

public class NameNode implements INameNode {
    private static final String FILE_PATH = "NameNode/";
    private HashMap<Integer, Entry> hashMap;

    private static final NameNode nameNode = new NameNode();
    public static NameNode getInstance() {
        return nameNode;
    }
    private NameNode() {
        hashMap = new HashMap<>();
        init();
    }

    private void init() {
        // load node info from disk
        File rootDir = new File(FILE_PATH);
        if (!rootDir.exists()) {
            rootDir.mkdirs();
        }
        // read entries in root directory node
        DirEntry rootEntry = new DirEntry("", 0);
        hashMap.put(0, rootEntry);
        File rootNodeFile = new File(FILE_PATH+"0.node");
        if (!rootNodeFile.exists()) {
            try {
                updateDirEntry(rootEntry);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            loadDirNode(rootEntry, 0);
        }
    }

    private void loadDirNode(DirEntry parentEntry, int id) {
        try {
            FileInputStream fis = new FileInputStream(FILE_PATH+id+".node");
            byte[] nodeSignByte = new byte[1];
            byte[] nodeNameLengthBytes = new byte[4];
            byte[] nodeIdBytes = new byte[4];
            while (fis.read(nodeSignByte) != -1) {
                fis.read(nodeNameLengthBytes);
                int nodeNameLength = ByteBuffer.wrap(nodeNameLengthBytes).getInt();
                byte[] nodeNameBytes = new byte[nodeNameLength];
                fis.read(nodeNameBytes);
                String nodeName = new String(nodeNameBytes);
                fis.read(nodeIdBytes);
                int nodeId = ByteBuffer.wrap(nodeIdBytes).getInt();
                if (nodeSignByte[0]==1) {
                    // it is a directory
                    DirEntry dirEntry = new DirEntry(nodeName, nodeId);
                    parentEntry.addEntry(dirEntry);
                    hashMap.put(nodeId, dirEntry);
                    loadDirNode(dirEntry, nodeId);
                } else {
                    // it is a file
                    FileEntry fileEntry = new FileEntry(nodeName, nodeId, loadFileNode(nodeId));
                    parentEntry.addEntry(fileEntry);
                    hashMap.put(nodeId, fileEntry);
                }
            }
            fis.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private FileNode loadFileNode(int id) {
        try {
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(FILE_PATH+id+".node"));
            return (FileNode) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    private int getNextID() {
        int i = 0;
        while(true) {
            if (hashMap.get(i) == null) {
                return i;
            }
            i++;
        }
    }

    private DirEntry locateDir(String fileUri) throws NoSuchFileException {
        // parse fileUri
        String[] parsedUri = fileUri.split("/");
        DirEntry currentRoot = (DirEntry) hashMap.get(0);
        int nextIndex = 1;
        while(nextIndex < parsedUri.length-1) {
            Entry temp = currentRoot.findChild(parsedUri[nextIndex]);
            if (temp == null || temp.getType() == Entry.NodeType.FILE) {
                throw new NoSuchFileException(fileUri);
            }
            currentRoot = (DirEntry) temp;
            nextIndex++;
        }
        return currentRoot;
    }

    private FileEntry locateFile(String fileUri) throws NoSuchFileException, FileNotFoundException {
        String fileName = fileUri.substring(fileUri.lastIndexOf('/')+1);
        DirEntry dirEntry = locateDir(fileUri);
        Entry entry = dirEntry.findChild(fileName);
        if (entry != null) {
            if (entry.getType() == Entry.NodeType.FILE) {
                return (FileEntry) entry;
            } else {
                throw new NoSuchFileException(fileUri);
            }
        } else {
            throw new FileNotFoundException();
        }
    }

    @Override
    public FileNode open(String fileUri) throws IOException {
        return locateFile(fileUri).getNode();
    }

    @Override
    public FileNode create(String fileUri) throws IOException {
        String fileName = fileUri.substring(fileUri.lastIndexOf('/')+1);
        DirEntry dirEntry = locateDir(fileUri);
        if (dirEntry.findChild(fileName) != null) {
            throw new FileAlreadyExistsException(fileUri);
        }
        FileNode fileNode = new FileNode();
        int id = getNextID();
        FileEntry fileEntry = new FileEntry(fileName, id, fileNode);
        dirEntry.addEntry(fileEntry);
        hashMap.put(id, fileEntry);
        updateFileEntry(fileEntry);
        updateDirEntry(dirEntry);
        return fileNode;
    }

    @Override
    public void close(String fileUri) throws IOException {
        FileEntry fileEntry = locateFile(fileUri);
        updateFileEntry(fileEntry);
        DirEntry dirEntry = locateDir(fileUri);
        updateDirEntry(dirEntry);
    }

    private void updateFileEntry(FileEntry fileEntry) throws IOException {
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(FILE_PATH+fileEntry.getId()+".node"));
        oos.writeObject(fileEntry.getNode());
        oos.close();
    }

    private void updateDirEntry(DirEntry dirEntry) throws IOException {
        byte[] bytes = dirEntry.toBytes();
        FileOutputStream fos = new FileOutputStream(FILE_PATH+dirEntry.getId()+".node");
        fos.write(bytes);
        fos.close();
    }

    @Override
    public void mkdir(String fileUri) throws IOException {
        String dirName = fileUri.substring(fileUri.lastIndexOf('/')+1);
        DirEntry dirEntry = locateDir(fileUri);
        if (dirEntry.findChild(dirName) != null) {
            throw new FileAlreadyExistsException(fileUri);
        }
        int id = getNextID();
        DirEntry newEntry = new DirEntry(dirName, id);
        dirEntry.addEntry(newEntry);
        hashMap.put(id, newEntry);
        updateDirEntry(dirEntry);
        updateDirEntry(newEntry);
    }


    @Override
    public LocatedBlock addBlock(String fileUri) {
        try {
            FileNode fileNode = open(fileUri);
            LocatedBlock locatedBlock = DataNode.getInstance().assignABlock();
            fileNode.addBlock(locatedBlock);
            return locatedBlock;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


}
