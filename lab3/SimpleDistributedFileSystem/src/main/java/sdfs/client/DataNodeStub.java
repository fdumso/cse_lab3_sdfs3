/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.exception.IllegalAccessTokenException;
import sdfs.packet.DataNodeRequest;
import sdfs.packet.DataNodeResponse;
import sdfs.protocol.IDataNodeProtocol;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.UUID;

public class DataNodeStub implements IDataNodeProtocol {
    private InetAddress address;
    private int port;

    DataNodeStub(InetAddress address, int port) {
        this.address = address;
        this.port = port;
    }

    /**
     * send request to DataNode and return the response
     * @param request sent to DataNode
     * @return response from DataNode
     */
    private DataNodeResponse sentRequest(DataNodeRequest request) {
        try {
            Socket socket = new Socket(address, port);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
            objectOutputStream.writeObject(request);
            objectOutputStream.flush();
            ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
            DataNodeResponse dataNodeResponse = (DataNodeResponse) objectInputStream.readObject();
            socket.close();
            return dataNodeResponse;
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
    public byte[] read(UUID fileAccessToken, int blockNumber, long position, int size) throws IllegalAccessTokenException, IllegalArgumentException {
        DataNodeRequest request = new DataNodeRequest(DataNodeRequest.Type.READ, fileAccessToken, blockNumber, position, size, null);
        DataNodeResponse response = sentRequest(request);
        assert response != null;
        if (response.getIllegalArgumentException() != null) {
            throw response.getIllegalArgumentException();
        }  else if (response.getIllegalAccessTokenException() != null) {
            throw response.getIllegalAccessTokenException();
        } else {
            return response.getData();
        }
    }

    @Override
    public void write(UUID fileAccessToken, int blockNumber, long position, byte[] buffer) throws IllegalAccessTokenException, IllegalArgumentException {
        DataNodeRequest request = new DataNodeRequest(DataNodeRequest.Type.WRITE, fileAccessToken, blockNumber, position, 0, buffer);
        DataNodeResponse response = sentRequest(request);
        assert response != null;
        if (response.getIllegalArgumentException() != null) {
            throw response.getIllegalArgumentException();
        } else if (response.getIllegalAccessTokenException() != null) {
            throw response.getIllegalAccessTokenException();
        }
    }
}
