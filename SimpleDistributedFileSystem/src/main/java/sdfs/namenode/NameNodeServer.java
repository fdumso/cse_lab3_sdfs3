package sdfs.namenode;

import sdfs.exception.IllegalAccessTokenException;
import sdfs.exception.SDFSFileAlreadyExistsException;
import sdfs.packet.NameNodeRequest;
import sdfs.packet.NameNodeResponse;
import sdfs.protocol.INameNodeProtocol;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class NameNodeServer {

    private NameNode nameNode;
    private ServerSocket serverSocket;

    public NameNodeServer(long flushDiskInternalSeconds) {
        this.nameNode = new NameNode(flushDiskInternalSeconds);


        try {
            this.serverSocket = new ServerSocket(INameNodeProtocol.NAME_NODE_PORT);
        } catch (IOException e) {
            System.err.println("Socket error!");
            e.printStackTrace();
        }


    }

    public void start() throws IOException {
        System.out.println("Server started!\nWaiting for request...");
        while (true) {
            Socket socketWithClient = serverSocket.accept();
            new Thread(new ClientHandler(socketWithClient)).start();
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
                        case NEW_COW_BLOCK: response = handleNewCOWBlock(request);
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
                SDFSFileChannelData sdfsFileChannelData = nameNode.openReadonly(fileUri);
                response.setSDFSFileChannelData(sdfsFileChannelData);
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
                SDFSFileChannelData sdfsFileChannelData = nameNode.openReadwrite(fileUri);
                response.setSDFSFileChannelData(sdfsFileChannelData);
            } catch (FileNotFoundException e) {
                response.setFileNotFoundException(new FileNotFoundException());
                e.printStackTrace();
            } catch (OverlappingFileLockException e) {
                response.setOverlappingFileLockException(new OverlappingFileLockException());
                e.printStackTrace();
            }
            return response;
        }

        NameNodeResponse handleCreate(NameNodeRequest request) {
            NameNodeResponse response = new NameNodeResponse(NameNodeResponse.Type.CREATE);
            String fileUri = request.getString();
            try {
                SDFSFileChannelData sdfsFileChannelData = nameNode.create(fileUri);
                response.setSDFSFileChannelData(sdfsFileChannelData);
            } catch (SDFSFileAlreadyExistsException e) {
                response.setSDFSFileAlreadyExistException(new SDFSFileAlreadyExistsException());
                e.printStackTrace();
            }
            return response;
        }

        NameNodeResponse handleCloseReadOnly(NameNodeRequest request) {
            NameNodeResponse response = new NameNodeResponse(NameNodeResponse.Type.CLOSE_READ_ONLY);
            UUID token = request.getToken();
            try {
                nameNode.closeReadonlyFile(token);
            } catch (IllegalAccessTokenException e) {
                response.setIllegalAccessTokenException(new IllegalAccessTokenException());
            }
            return response;
        }

        NameNodeResponse handleCloseReadWrite(NameNodeRequest request) {
            NameNodeResponse response = new NameNodeResponse(NameNodeResponse.Type.CLOSE_READ_WRITE);
            UUID token = request.getToken();
            long newFileSize = request.getNumber();
            try {
                nameNode.closeReadwriteFile(token, newFileSize);
            } catch (IllegalArgumentException e) {
                response.setIllegalArgumentException(new IllegalArgumentException());
            } catch (IllegalAccessTokenException e) {
                response.setIllegalAccessTokenException(new IllegalAccessTokenException());
            }
            return response;
        }

        NameNodeResponse handleMkdir(NameNodeRequest request) {
            NameNodeResponse response = new NameNodeResponse(NameNodeResponse.Type.MK_DIR);
            String fileUri = request.getString();
            try {
                nameNode.mkdir(fileUri);
            } catch (SDFSFileAlreadyExistsException e) {
                response.setSDFSFileAlreadyExistException(new SDFSFileAlreadyExistsException());
            }
            return response;
        }

        NameNodeResponse handleAddBlocks(NameNodeRequest request) {
            NameNodeResponse response = new NameNodeResponse(NameNodeResponse.Type.ADD_BLOCKS);
            UUID token = request.getToken();
            int blockAmount = (int) request.getNumber(); // cast long to integer
            try {
                List<LocatedBlock> blockList = nameNode.addBlocks(token, blockAmount);
                response.setBlockList(blockList);
            } catch (IllegalAccessTokenException e) {
                response.setIllegalAccessTokenException(new IllegalAccessTokenException());
            }
            return response;
        }

        NameNodeResponse handleRemoveLastBlocks(NameNodeRequest request) {
            NameNodeResponse response = new NameNodeResponse(NameNodeResponse.Type.REMOVE_LAST_BLOCKS);
            UUID token = request.getToken();
            int blockAmount = (int) request.getNumber(); // cast long to integer
            try {
                nameNode.removeLastBlocks(token, blockAmount);
            } catch (IllegalAccessTokenException e) {
                response.setIllegalAccessTokenException(new IllegalAccessTokenException());
            } catch (IndexOutOfBoundsException e) {
                response.setIndexOutOfBoundsException(new IndexOutOfBoundsException());
            }
            return response;
        }

        private NameNodeResponse handleNewCOWBlock(NameNodeRequest request) {
            NameNodeResponse response = new NameNodeResponse(NameNodeResponse.Type.NEW_COW_BLOCK);
            UUID token = request.getToken();
            int fileBlockNumber = (int) request.getNumber(); // cast long to integer
            try {
                LocatedBlock block = nameNode.newCopyOnWriteBlock(token, fileBlockNumber);
                List<LocatedBlock> blockList = new ArrayList<>();
                blockList.add(block);
                response.setBlockList(blockList);
            } catch (IllegalStateException e) {
                response.setIllegalStateException(new IllegalStateException());
            }
            return response;
        }
    }
}
