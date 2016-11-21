package sdfs.datanode;

import sdfs.exception.IllegalAccessTokenException;
import sdfs.packet.DataNodeRequest;
import sdfs.packet.DataNodeResponse;
import sdfs.protocol.IDataNodeProtocol;
import sdfs.protocol.INameNodeDataNodeProtocol;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.UUID;

public class DataNodeServer {
    private DataNode dataNode;
    private ServerSocket serverSocket;

    public DataNodeServer() throws IOException {
        this.dataNode = new DataNode();
        this.serverSocket = new ServerSocket(DataNode.DATA_NODE_PORT);
    }

    private void start() throws IOException {
        System.out.println("server started\nwaiting for request...");
        while (true) {
            Socket socketWithClient = serverSocket.accept();
            new Thread(new ClientHandler(socketWithClient)).start();
        }
    }

    public static void main(String[] args) {
        try {
            new DataNodeServer().start();
        } catch (IOException e) {
            e.printStackTrace();
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
            DataNodeResponse.Type type = DataNodeResponse.Type.READ;
            DataNodeResponse response = new DataNodeResponse(type);
            UUID uuid = request.getUuid();
            int blockNumber = request.getBlockNumber();
            long offset = request.getPosition();
            int size = request.getSize();
            try {
                byte[] data = dataNode.read(uuid, blockNumber, offset, size);
                response.setData(data);
            } catch (IllegalAccessTokenException e) {
                response.setIllegalAccessTokenException(new IllegalAccessTokenException());
            } catch (IllegalArgumentException e) {
                response.setIllegalArgumentException(new IllegalArgumentException());
            }
            return response;
        }

        DataNodeResponse handleWrite(DataNodeRequest request) {
            DataNodeResponse.Type type = DataNodeResponse.Type.WRITE;
            DataNodeResponse response = new DataNodeResponse(type);
            UUID uuid = request.getUuid();
            int blockNumber = request.getBlockNumber();
            int offset = request.getOffset();
            byte[] data = request.getData();
            try {
                dataNode.write(uuid, blockNumber, offset, data);
            } catch (IllegalStateException e) {
                response.setIllegalStateException(new IllegalStateException());
            } catch (IndexOutOfBoundsException e) {
                response.setIndexOutOfBoundsException(new IndexOutOfBoundsException());
            }
            return response;
        }
    }
}