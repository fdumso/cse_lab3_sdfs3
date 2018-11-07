/*
 * Copyright (c) Jipzingking 2016.
 */

package client;

import protocol.IDataNodeProtocol;
import request.DataNodeRequest;
import response.DataNodeResponse;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.UUID;

public class DataNodeStub implements IDataNodeProtocol {
    private Socket socketWithServer;

    public DataNodeStub(InetAddress inetAddress) {
        try {
            socketWithServer = new Socket(inetAddress, IDataNodeProtocol.DATA_NODE_PORT);
        } catch (IOException e) {
            System.err.println("Can not connect to DataNode!");
            e.printStackTrace();
        }
    }

    private DataNodeResponse sentRequest(DataNodeRequest request) {
        try {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socketWithServer.getOutputStream());
            objectOutputStream.writeObject(request);
            objectOutputStream.flush();
            ObjectInputStream objectInputStream = new ObjectInputStream(socketWithServer.getInputStream());
            return (DataNodeResponse) objectInputStream.readObject();
        } catch (IOException e) {
            System.err.println("Socket error!");
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            System.err.println("Illegal response!");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public byte[] read(UUID fileUuid, int blockNumber, int offset, int size) throws IllegalStateException, IndexOutOfBoundsException, FileNotFoundException {
        DataNodeRequest request = new DataNodeRequest(DataNodeRequest.Type.READ, fileUuid, blockNumber, offset, size, null);
        DataNodeResponse response = sentRequest(request);
        if (response.getFileNotFoundException() != null) {
            throw response.getFileNotFoundException();
        }  else if (response.getIllegalStateException() != null) {
            throw response.getIllegalStateException();
        } else if (response.getIndexOutOfBoundsException() != null) {
            throw response.getIndexOutOfBoundsException();
        } else {
            return response.getData();
        }
    }

    @Override
    public void write(UUID fileUuid, int blockNumber, int offset, byte[] b) throws IllegalStateException, IndexOutOfBoundsException {
        DataNodeRequest request = new DataNodeRequest(DataNodeRequest.Type.WRITE, fileUuid, blockNumber, offset, 0, b);
        DataNodeResponse response = sentRequest(request);
        if (response.getIllegalStateException() != null) {
            throw response.getIllegalStateException();
        } else if (response.getIndexOutOfBoundsException() != null) {
            throw response.getIndexOutOfBoundsException();
        }
    }
}
