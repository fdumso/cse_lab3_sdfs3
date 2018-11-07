/*
 * Copyright (c) Jipzingking 2016.
 */

package client;

import entity.LocatedBlock;
import entity.SDFSFileChannel;
import protocol.INameNodeProtocol;
import request.NameNodeRequest;
import response.NameNodeResponse;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.FileAlreadyExistsException;
import java.util.List;
import java.util.UUID;

public class NameNodeStub implements INameNodeProtocol {
    private Socket socketWithServer;

    public NameNodeStub(InetSocketAddress nameNodeAddress) {
        try {
            this.socketWithServer = new Socket(nameNodeAddress.getAddress(), INameNodeProtocol.NAME_NODE_PORT);
        } catch (IOException e) {
            System.err.println("Can not connect to NameNode!");
            e.printStackTrace();
        }
    }

    private NameNodeResponse sentRequest(NameNodeRequest request) {
        try {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socketWithServer.getOutputStream());
            objectOutputStream.writeObject(request);
            objectOutputStream.flush();
            ObjectInputStream objectInputStream = new ObjectInputStream(socketWithServer.getInputStream());
            return (NameNodeResponse) objectInputStream.readObject();
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
    public SDFSFileChannel openReadonly(String fileUri) throws FileNotFoundException {
        NameNodeRequest request = new NameNodeRequest(NameNodeRequest.Type.OPEN_READ_ONLY, fileUri, null, 0);
        NameNodeResponse response = sentRequest(request);
        if (response.getFileNotFoundException() != null) {
            throw response.getFileNotFoundException();
        } else {
            SDFSFileChannel sdfsFileChannel = response.getSdfsFileChannel();
            return sdfsFileChannel;
        }
    }

    @Override
    public SDFSFileChannel openReadwrite(String fileUri) throws FileNotFoundException, IllegalStateException {
        NameNodeRequest request = new NameNodeRequest(NameNodeRequest.Type.OPEN_READ_WRITE, fileUri, null, 0);
        NameNodeResponse response = sentRequest(request);
        if (response.getFileNotFoundException() != null) {
            throw response.getFileNotFoundException();
        } else if (response.getIllegalStateException() != null) {
            throw response.getIllegalStateException();
        } else {
            SDFSFileChannel sdfsFileChannel = response.getSdfsFileChannel();
            return sdfsFileChannel;
        }
    }

    @Override
    public SDFSFileChannel create(String fileUri) throws FileAlreadyExistsException {
        NameNodeRequest request = new NameNodeRequest(NameNodeRequest.Type.CREATE, fileUri, null, 0);
        NameNodeResponse response = sentRequest(request);
        if (response.getFileAlreadyExistsException() != null) {
            throw response.getFileAlreadyExistsException();
        } else {
            SDFSFileChannel sdfsFileChannel = response.getSdfsFileChannel();
            return sdfsFileChannel;
        }
    }

    @Override
    public void closeReadonlyFile(UUID fileUuid) throws IllegalStateException {
        NameNodeRequest request = new NameNodeRequest(NameNodeRequest.Type.CLOSE_READ_ONLY, null, fileUuid, 0);
        NameNodeResponse response = sentRequest(request);
        if (response.getIllegalStateException() != null) {
            throw response.getIllegalStateException();
        }
    }

    @Override
    public void closeReadwriteFile(UUID fileUuid, int newFileSize) throws IllegalStateException, IllegalArgumentException {
        NameNodeRequest request = new NameNodeRequest(NameNodeRequest.Type.CLOSE_READ_WRITE, null, fileUuid, newFileSize);
        NameNodeResponse response = sentRequest(request);
        if (response.getIllegalStateException() != null) {
            throw response.getIllegalStateException();
        } else if (response.getIllegalArgumentException() != null) {
            throw response.getIllegalArgumentException();
        }
    }

    @Override
    public void mkdir(String fileUri) throws FileAlreadyExistsException {
        NameNodeRequest request = new NameNodeRequest(NameNodeRequest.Type.MK_DIR, fileUri, null, 0);
        NameNodeResponse response = sentRequest(request);
        if (response.getFileAlreadyExistsException() != null) {
            throw response.getFileAlreadyExistsException();
        }
    }

    @Override
    public LocatedBlock addBlock(UUID fileUuid) throws IllegalStateException {
        return addBlocks(fileUuid, 1).get(0);
    }

    @Override
    public List<LocatedBlock> addBlocks(UUID fileUuid, int blockAmount) throws IllegalStateException {
        NameNodeRequest request = new NameNodeRequest(NameNodeRequest.Type.ADD_BLOCKS, null, fileUuid, blockAmount);
        NameNodeResponse response = sentRequest(request);
        if (response.getIllegalStateException() != null) {
            throw response.getIllegalStateException();
        } else {
            return response.getBlockList();
        }
    }

    @Override
    public void removeLastBlock(UUID fileUuid) throws IllegalStateException {
        removeLastBlocks(fileUuid, 1);
    }

    @Override
    public void removeLastBlocks(UUID fileUuid, int blockAmount) throws IllegalStateException {
        NameNodeRequest request = new NameNodeRequest(NameNodeRequest.Type.REMOVE_LAST_BLOCKS, null, fileUuid, blockAmount);
        NameNodeResponse response = sentRequest(request);
        if (response.getIllegalStateException() != null) {
            throw response.getIllegalStateException();
        }
    }
}
