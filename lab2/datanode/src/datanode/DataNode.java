/*
 * Copyright (c) Jipzingking 2016.
 */

package datanode;

import protocol.IDataNodeProtocol;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.UUID;

public class DataNode implements IDataNodeProtocol {
    /**
     * The block size may be changed during test.
     * So please use this constant.
     */
    public static final int BLOCK_SIZE = 64 * 1024;
    public static final int DATA_NODE_PORT = 4341;
    //    put off due to its difficulties
    //    private final Map<UUID, Set<Integer>> uuidReadonlyPermissionCache = new HashMap<>();
    //    private final Map<UUID, Set<Integer>> uuidReadwritePermissionCache = new HashMap<>();

    public static final String FILE_PATH = "datanode/res/";

    public DataNode() {
        File rootDir = new File(FILE_PATH);
        if (!rootDir.exists()) {
            rootDir.mkdir();
        }
    }

    @Override
    public byte[] read(UUID fileUuid, int blockNumber, int offset, int size) throws IllegalStateException, IndexOutOfBoundsException, FileNotFoundException {
        File blockFile = new File(FILE_PATH+blockNumber+".block");
        if (!blockFile.exists()) {
            throw new FileNotFoundException();
        }
        if (offset < 0 || offset+size > BLOCK_SIZE) {
            throw  new IndexOutOfBoundsException();
        }
        FileInputStream fileInputStream = new FileInputStream(blockFile);
        byte[] allData = new byte[offset+size];
        try {
            fileInputStream.read(allData);
            fileInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(allData);
        byte[] data = new byte[size];
        byteBuffer.get(data, offset, size);
        return data;
    }

    @Override
    public void write(UUID fileUuid, int blockNumber, int offset, byte[] b) throws IllegalStateException, IndexOutOfBoundsException {
        File blockFile = new File(FILE_PATH+blockNumber+".block");
        if (!blockFile.exists()) {
            try {
                blockFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (offset < 0 || offset+b.length > BLOCK_SIZE) {
            throw  new IndexOutOfBoundsException();
        }
        try {
            if (offset!=0) {
                byte[] old = new byte[offset];
                FileInputStream fileInputStream = new FileInputStream(blockFile);
                fileInputStream.read(old, 0, offset);
                fileInputStream.close();
                ByteBuffer byteBuffer = ByteBuffer.allocate(offset+b.length);
                byteBuffer.put(old);
                byteBuffer.put(b);
                FileOutputStream fileOutputStream = new FileOutputStream(blockFile);
                fileOutputStream.write(byteBuffer.array());
                fileOutputStream.close();
            } else {
                FileOutputStream fileOutputStream = new FileOutputStream(blockFile);
                fileOutputStream.write(b);
                fileOutputStream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
