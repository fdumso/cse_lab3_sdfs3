package sdfs.datanode;

import sdfs.entity.LocatedBlock;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.util.HashMap;

public class DataNode implements IDataNode {
    private static final int BLOCK_SIZE = 64 * 1024;
    private static final String FILE_PATH = "DataNode/";

    private InetAddress address;
    private HashMap<Integer, LocatedBlock> hashMap;
    private static final DataNode dataNode = new DataNode();
    public static DataNode getInstance() {
        return dataNode;
    }
    private DataNode() {
        this.hashMap = new HashMap<>();
        try {
            this.address = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        File rootDir = new File(FILE_PATH);
        if (!rootDir.exists()) {
            rootDir.mkdirs();
        }
    }

    @Override
    public int read(int blockNumber, int offset, int size, byte[] b) throws IndexOutOfBoundsException, FileNotFoundException, IOException {
        if (offset < 0 || offset+size > BLOCK_SIZE || b.length < size) {
            throw new IndexOutOfBoundsException();
        }
        byte[] all = new byte[offset+size];
        FileInputStream fis = new FileInputStream(FILE_PATH+blockNumber+".block");
        fis.read(all, 0, offset+size);
        ByteBuffer byteBuffer = ByteBuffer.wrap(all);
        fis.close();
        byteBuffer.get(b, offset, size);
        return size;
    }

    @Override
    public void write(int blockNumber, int offset, int size, byte[] b) throws IndexOutOfBoundsException, FileAlreadyExistsException, IOException {
        if (offset < 0 || offset+size > BLOCK_SIZE || b.length < size) {
            throw new IndexOutOfBoundsException();
        }
        if (offset!=0) {
            byte[] old = new byte[offset];
            FileInputStream fis = new FileInputStream(FILE_PATH+blockNumber+".block");
            fis.read(old, 0, offset);
            fis.close();
            ByteBuffer byteBuffer = ByteBuffer.allocate(offset+size);
            byteBuffer.put(old);
            byteBuffer.put(b,0,size);
            FileOutputStream fos = new FileOutputStream(FILE_PATH+blockNumber+".block");
            fos.write(byteBuffer.array(), 0, offset+size);
            fos.close();
            hashMap.get(blockNumber).setSize(offset+size);
        } else {
            FileOutputStream fos = new FileOutputStream(FILE_PATH+blockNumber+".block");
            fos.write(b, 0, size);
            fos.close();
            hashMap.get(blockNumber).setSize(size);
        }
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

    public LocatedBlock assignABlock() {
        int id = getNextID();
        LocatedBlock locatedBlock = new LocatedBlock(address, id);
        hashMap.put(id, locatedBlock);
        return locatedBlock;
    }

    public static int getBlockSize() {
        return BLOCK_SIZE;
    }

}
