package sdfs.datanode;

import sdfs.exception.IllegalAccessTokenException;
import sdfs.protocol.IDataNodeProtocol;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.UUID;

public class DataNode implements IDataNodeProtocol {

    private static final String FILE_PATH = "datanode/";

    public DataNode() {
        File rootDir = new File(FILE_PATH);
        if (!rootDir.exists()) {
            rootDir.mkdir();
        }
    }

    @Override
    public byte[] read(UUID fileAccessToken, int blockNumber, long position, int size) throws IllegalAccessTokenException, IllegalArgumentException {
        File blockFile = new File(FILE_PATH+blockNumber+".block");
        if (position < 0 || position+size > BLOCK_SIZE) {
            throw new IllegalArgumentException();
        }
        FileInputStream fileInputStream = null;
        try {
            fileInputStream = new FileInputStream(blockFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        byte[] allData = new byte[(int)position+size];
        int readSize = 0;
        try {
            assert fileInputStream != null;
            readSize = fileInputStream.read(allData);
            fileInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(allData);
        byte[] data = new byte[readSize];
        byteBuffer.get(data, (int)position, size);
        return data;
    }

    @Override
    public void write(UUID fileAccessToken, int blockNumber, long position, byte[] buffer) throws IllegalAccessTokenException, IllegalArgumentException {
        File blockFile = new File(FILE_PATH+blockNumber+".block");
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
            // if write at the middle of the block
            // need to load original data
            if (offset!=0) {
                byte[] old = new byte[offset];
                FileInputStream fileInputStream = new FileInputStream(blockFile);
                int readSize = fileInputStream.read(old, 0, offset);
                fileInputStream.close();
                ByteBuffer byteBuffer = ByteBuffer.allocate(offset+buffer.length);
                byteBuffer.put(old);
                byteBuffer.put(buffer);
                FileOutputStream fileOutputStream = new FileOutputStream(blockFile);
                fileOutputStream.write(byteBuffer.array());
                fileOutputStream.close();
            } else {
                FileOutputStream fileOutputStream = new FileOutputStream(blockFile);
                fileOutputStream.write(buffer);
                fileOutputStream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
