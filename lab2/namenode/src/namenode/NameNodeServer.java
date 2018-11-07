package namenode;

import entity.LocatedBlock;
import entity.SDFSFileChannel;
import request.NameNodeRequest;
import response.NameNodeResponse;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.FileAlreadyExistsException;
import java.util.List;
import java.util.UUID;

public class NameNodeServer {
    private NameNode nameNode;
    private ServerSocket serverSocket;

    public NameNodeServer() throws IOException {
        this.nameNode = new NameNode();
        this.serverSocket = new ServerSocket(NameNode.NAME_NODE_PORT);
    }

    public void start() throws IOException {
        System.out.println("server started\nwaiting for request...");
        while (true) {
            Socket socketWithClient = serverSocket.accept();
            new Thread(new ClientHandler(socketWithClient)).start();
        }
    }

    public static void main(String[] args) {
        try {
            new NameNodeServer().start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private class ClientHandler implements Runnable {
        private Socket socketWithClient;

        public ClientHandler(Socket socketWithClient) {
            this.socketWithClient = socketWithClient;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    // deserialize request
                    ObjectInputStream objectInputStream = new ObjectInputStream(socketWithClient.getInputStream());
                    NameNodeRequest request = (NameNodeRequest) objectInputStream.readObject();
                    NameNodeResponse response;
                    // switch request type
                    switch (request.getType()) {
                        case OPEN_READ_ONLY: response = handleOpenReadOnly(request);
                            break;
                        case OPEN_READ_WRITE: response = handleOpenReadWrite(request);
                            break;
                        case CLOSE_READ_ONLY: response = handleCloseReadOnly(request);
                            break;
                        case CLOSE_READ_WRITE: response = handleCloseReadWrite(request);
                            break;
                        case CREATE: response = handleCreate(request);
                            break;
                        case MK_DIR: response = handleMkdir(request);
                            break;
                        case ADD_BLOCKS: response = handleAddBlocks(request);
                            break;
                        case REMOVE_LAST_BLOCKS: response = handleRemoveLastBlocks(request);
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

        NameNodeResponse handleOpenReadOnly(NameNodeRequest request) {
            NameNodeResponse response = new NameNodeResponse(NameNodeResponse.Type.OPEN_READ_ONLY);
            String fileUri = request.getString();
            try {
                SDFSFileChannel sdfsFileChannel = nameNode.openReadonly(fileUri);
                response.setSdfsFileChannel(sdfsFileChannel);
            } catch (FileNotFoundException e) {
                response.setFileNotFoundException(new FileNotFoundException());
                e.printStackTrace();
            }
            return response;
        }

        NameNodeResponse handleOpenReadWrite(NameNodeRequest request) {
            NameNodeResponse response = new NameNodeResponse(NameNodeResponse.Type.OPEN_READ_WRITE);
            String fileUri = request.getString();
            try {
                SDFSFileChannel sdfsFileChannel = nameNode.openReadwrite(fileUri);
                response.setSdfsFileChannel(sdfsFileChannel);
            } catch (FileNotFoundException e) {
                response.setFileNotFoundException(new FileNotFoundException());
                e.printStackTrace();
            } catch (IllegalStateException e) {
                response.setIllegalStateException(new IllegalStateException());
                e.printStackTrace();
            }
            return response;
        }

        NameNodeResponse handleCreate(NameNodeRequest request) {
            NameNodeResponse response = new NameNodeResponse(NameNodeResponse.Type.CREATE);
            String fileUri = request.getString();
            try {
                SDFSFileChannel sdfsFileChannel = nameNode.create(fileUri);
                response.setSdfsFileChannel(sdfsFileChannel);
            } catch (FileAlreadyExistsException e) {
                response.setFileAlreadyExistsException(new FileAlreadyExistsException(null));
                e.printStackTrace();
            }
            return response;
        }

        NameNodeResponse handleCloseReadOnly(NameNodeRequest request) {
            NameNodeResponse response = new NameNodeResponse(NameNodeResponse.Type.CLOSE_READ_ONLY);
            UUID uuid = request.getUuid();
            try {
                nameNode.closeReadonlyFile(uuid);
            } catch (IllegalStateException e) {
                response.setIllegalStateException(new IllegalStateException());
            }
            return response;
        }

        NameNodeResponse handleCloseReadWrite(NameNodeRequest request) {
            NameNodeResponse response = new NameNodeResponse(NameNodeResponse.Type.CLOSE_READ_WRITE);
            UUID uuid = request.getUuid();
            int newFileSize = request.getInteger();
            try {
                nameNode.closeReadwriteFile(uuid, newFileSize);
            } catch (IllegalStateException e) {
                response.setIllegalStateException(new IllegalStateException());
            } catch (IllegalArgumentException e) {
                response.setIllegalArgumentException(new IllegalArgumentException());
            }
            return response;
        }

        NameNodeResponse handleMkdir(NameNodeRequest request) {
            NameNodeResponse response = new NameNodeResponse(NameNodeResponse.Type.MK_DIR);
            String fileUri = request.getString();
            try {
                nameNode.mkdir(fileUri);
            } catch (FileAlreadyExistsException e) {
                response.setFileAlreadyExistsException(new FileAlreadyExistsException(null));
            }
            return response;
        }

        NameNodeResponse handleAddBlocks(NameNodeRequest request) {
            NameNodeResponse response = new NameNodeResponse(NameNodeResponse.Type.ADD_BLOCKS);
            UUID uuid = request.getUuid();
            int blockAmount = request.getInteger();
            try {
                List<LocatedBlock> blockList = nameNode.addBlocks(uuid, blockAmount);
                response.setBlockList(blockList);
            } catch (IllegalStateException e) {
                response.setIllegalStateException(new IllegalStateException());
            }
            return response;
        }

        NameNodeResponse handleRemoveLastBlocks(NameNodeRequest request) {
            NameNodeResponse response = new NameNodeResponse(NameNodeResponse.Type.REMOVE_LAST_BLOCKS);
            UUID uuid = request.getUuid();
            int blockAmount = request.getInteger();
            try {
                nameNode.removeLastBlocks(uuid, blockAmount);
            } catch (IllegalStateException e) {
                response.setIllegalStateException(new IllegalStateException());
            }
            return response;
        }
    }
}
