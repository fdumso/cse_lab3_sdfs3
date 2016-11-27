package sdfs.datanode;

import sdfs.exception.IllegalAccessTokenException;
import sdfs.namenode.AccessTokenPermission;
import sdfs.protocol.IDataNodeProtocol;

import java.io.*;
import java.net.InetAddress;
import java.util.UUID;

public class DataNode implements IDataNodeProtocol {
    private static final String FILE_PATH = System.getProperty("sdfs.datanode.dir");
    public static final int BLOCK_SIZE = 64 * 1024;

    private final NameNodeStub nameNodeStub;

    DataNode(InetAddress nameNodeAddress, int nameNodePort) {
        this.nameNodeStub = new NameNodeStub(nameNodeAddress, nameNodePort);
    }

    @Override
    public byte[] read(UUID token, int blockID, long position, int size) throws IllegalAccessTokenException, IllegalArgumentException {
        // ask name node access token
        AccessTokenPermission accessTokenPermission = nameNodeStub.getAccessTokenPermission(token, null);
        if (accessTokenPermission == null) {
            throw new IllegalAccessTokenException();
        }
        if (!accessTokenPermission.getAllowedBlocks().contains(blockID)) {
            throw new IllegalAccessTokenException();
        }
        if (position < 0 || position+size > BLOCK_SIZE) {
            throw new IllegalArgumentException();
        }

        File blockFile = new File(FILE_PATH + blockID+".block");
        FileInputStream fileInputStream;
        try {
            fileInputStream = new FileInputStream(blockFile);
        } catch (FileNotFoundException e) {
            return new byte[0];
        }
        byte[] allData = new byte[(int)position+size];
        int readSize = 0;
        try {
            readSize = fileInputStream.read(allData);
            fileInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        size = Math.min(size, readSize-(int)position);
        byte[] data = new byte[size];
        System.arraycopy(allData, (int) position, data, 0, size);
        return data;
    }

    @Override
    public void write(UUID token, int blockID, long position, byte[] buffer) throws IllegalAccessTokenException, IllegalArgumentException {
        // ask name node access token
        AccessTokenPermission accessTokenPermission = nameNodeStub.getAccessTokenPermission(token, null);
        if (accessTokenPermission == null || !accessTokenPermission.isWritable()) {
            throw new IllegalAccessTokenException();
        }
        if (!accessTokenPermission.getAllowedBlocks().contains(blockID)) {
            throw new IllegalAccessTokenException();
        }

        File blockFile = new File(FILE_PATH + blockID+".block");
        if (!blockFile.exists()) {
            try {
                blockFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        int offset = (int) position;
        if (offset < 0 || offset+buffer.length > BLOCK_SIZE) {
            throw new IllegalArgumentException();
        }
        try {
            FileInputStream fileInputStream = new FileInputStream(blockFile);
            byte[] oldData = new byte[offset];
            int readSize = fileInputStream.read(oldData);
            fileInputStream.close();
            // if old data
            // need to load original data
            byte[] newData = new byte[offset+buffer.length];
            if (readSize != -1) {
                System.arraycopy(oldData, 0, newData, 0, readSize);
                for (int i = readSize; i < offset; i++) {
                    newData[i] = 0;
                }
            } else {
                for (int i = 0; i < offset; i++) {
                    newData[i] = 0;
                }
            }
            System.arraycopy(buffer, 0, newData, offset, buffer.length);
            FileOutputStream fileOutputStream = new FileOutputStream(blockFile);
            fileOutputStream.write(newData);
            fileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
