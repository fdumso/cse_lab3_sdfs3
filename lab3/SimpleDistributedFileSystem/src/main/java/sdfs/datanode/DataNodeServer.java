package sdfs.datanode;

import sdfs.exception.IllegalAccessTokenException;
import sdfs.packet.DataNodeRequest;
import sdfs.packet.DataNodeResponse;
import sdfs.protocol.SDFSConfiguration;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.UUID;

public class DataNodeServer implements Runnable {
    private DataNode dataNode;
    private ServerSocket serverSocket;

    public DataNodeServer(SDFSConfiguration configuration) {
        this.dataNode = new DataNode(configuration.getNameNodeAddress(), configuration.getNameNodePort());
        try {
            this.serverSocket = new ServerSocket(configuration.getDataNodePort());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public DataNode getDataNode() {
        return dataNode;
    }

    @Override
    public void run() {
        while (true) {
            Socket socketWithClient = null;
            try {
                socketWithClient = serverSocket.accept();
            } catch (IOException e) {
                e.printStackTrace();
            }
            new Thread(new ClientHandler(socketWithClient)).start();
        }
    }

    private class ClientHandler implements Runnable {
        private Socket socketWithClient;

        ClientHandler(Socket socketWithClient) {
            this.socketWithClient = socketWithClient;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    // deserialize request
                    InputStream inputStream = socketWithClient.getInputStream();
                    ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                    DataNodeRequest request = (DataNodeRequest) objectInputStream.readObject();
                    DataNodeResponse response;
                    // switch request type
                    switch (request.getType()) {
                        case READ:
                            response = handleRead(request);
                            break;
                        case WRITE:
                            response = handleWrite(request);
                            break;
                        default: // ignore this request
                            return;
                    }
                    // send response
                    OutputStream outputStream = socketWithClient.getOutputStream();
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                    objectOutputStream.writeObject(response);
                    objectOutputStream.flush();
                }
            } catch (IOException | ClassNotFoundException | NullPointerException ignored) {
            }

        }

        DataNodeResponse handleRead(DataNodeRequest request) {
            DataNodeResponse response = new DataNodeResponse();
            UUID token = request.getToken();
            int blockNumber = request.getBlockNumber();
            long offset = request.getPosition();
            int size = request.getSize();
            try {
                byte[] data = dataNode.read(token, blockNumber, offset, size);
                response.setData(data);
            } catch (IllegalAccessTokenException e) {
                response.setIllegalAccessTokenException(e);
            } catch (IllegalArgumentException e) {
                response.setIllegalArgumentException(e);
            }
            return response;
        }

        DataNodeResponse handleWrite(DataNodeRequest request) {
            DataNodeResponse response = new DataNodeResponse();
            UUID token = request.getToken();
            int blockNumber = request.getBlockNumber();
            long offset = request.getPosition();
            byte[] data = request.getData();
            try {
                dataNode.write(token, blockNumber, offset, data);
            } catch (IllegalAccessTokenException e) {
                response.setIllegalAccessTokenException(e);
            } catch (IllegalArgumentException e) {
                response.setIllegalArgumentException(e);
            }
            return response;
        }
    }
}