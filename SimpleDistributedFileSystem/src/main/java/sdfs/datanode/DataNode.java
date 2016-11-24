package sdfs.datanode;

import sdfs.exception.IllegalAccessTokenException;
import sdfs.namenode.AccessTokenPermission;
import sdfs.packet.NameNodeRequest;
import sdfs.packet.NameNodeResponse;
import sdfs.protocol.IDataNodeProtocol;
import sdfs.protocol.INameNodeDataNodeProtocol;
import sdfs.protocol.INameNodeProtocol;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;

public class DataNode implements IDataNodeProtocol, INameNodeDataNodeProtocol {

    private static final String FILE_PATH = System.getProperty("sdfs.datanode.dir");

    private Socket socketWithNameNode;

    public DataNode() {
        File rootDir = new File(FILE_PATH);
        if (!rootDir.exists()) {
            rootDir.mkdir();
        }

        try {
            this.socketWithNameNode = new Socket(InetAddress.getLocalHost(), INameNodeProtocol.NAME_NODE_PORT);
        } catch (IOException e) {
            System.err.println("Can not connect to NameNode!");
            e.printStackTrace();
        }
    }


    @Override
    public AccessTokenPermission getAccessTokenPermission(UUID token) {
        NameNodeRequest request = new NameNodeRequest(NameNodeRequest.Type.GET_ACCESS_TOKEN_PERMISSION, null, token, 0);
        NameNodeResponse response = null;
        try {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socketWithNameNode.getOutputStream());
            objectOutputStream.writeObject(request);
            objectOutputStream.flush();
            ObjectInputStream objectInputStream = new ObjectInputStream(socketWithNameNode.getInputStream());
            response = (NameNodeResponse) objectInputStream.readObject();
        } catch (IOException e) {
            System.err.println("Socket error!");
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            System.err.println("Illegal response!");
            e.printStackTrace();
        }
        assert response != null;
        return response.getAccessTokenPermission();
    }

    @Override
    public byte[] read(UUID token, int blockNumber, long position, int size) throws IllegalAccessTokenException, IllegalArgumentException {
        // ask name node access token
        AccessTokenPermission accessTokenPermission = getAccessTokenPermission(token);
        if (accessTokenPermission == null) {
            throw new IllegalAccessTokenException();
        }
        if (!accessTokenPermission.getAllowedBlocks().contains(blockNumber)) {
            throw new IllegalAccessTokenException();
        }
        if (position < 0 || position+size > BLOCK_SIZE) {
            throw new IllegalArgumentException();
        }

        File blockFile = new File(FILE_PATH+blockNumber+".block");
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
        for (int i = 0; i < size; i++) {
            data[i] = allData[(int)position+i];
        }
        return data;
    }

    @Override
    public void write(UUID token, int blockNumber, long position, byte[] buffer) throws IllegalAccessTokenException, IllegalArgumentException {
        // ask name node access token
        AccessTokenPermission accessTokenPermission = getAccessTokenPermission(token);
        if (accessTokenPermission == null || !accessTokenPermission.isWritable()) {
            throw new IllegalAccessTokenException();
        }
        if (!accessTokenPermission.getAllowedBlocks().contains(blockNumber)) {
            throw new IllegalAccessTokenException();
        }

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
            FileInputStream fileInputStream = new FileInputStream(blockFile);
            byte[] oldData = new byte[offset];
            int readSize = fileInputStream.read(oldData);
            fileInputStream.close();
            // if old data
            // need to load original data
            byte[] newData = new byte[offset+buffer.length];
            if (readSize != -1) {
                for (int i = 0; i < readSize; i++) {
                    newData[i] = oldData[i];
                }
                for (int i = readSize; i < offset; i++) {
                    newData[i] = 0;
                }
            } else {
                for (int i = 0; i < offset; i++) {
                    newData[i] = 0;
                }
            }
            for (int i = 0; i < buffer.length; i++) {
                newData[offset+i] = buffer[i];
            }
            FileOutputStream fileOutputStream = new FileOutputStream(blockFile);
            fileOutputStream.write(newData);
            fileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
