package sdfs.client;

import sdfs.exception.IllegalAccessTokenException;
import sdfs.exception.SDFSFileAlreadyExistsException;
import sdfs.filetree.LocatedBlock;
import sdfs.namenode.SDFSFileChannelData;
import sdfs.packet.NameNodeRequest;
import sdfs.packet.NameNodeResponse;
import sdfs.protocol.INameNodeProtocol;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.channels.OverlappingFileLockException;
import java.util.List;
import java.util.UUID;

public class NameNodeStub implements INameNodeProtocol {

    private Socket socketWithServer;

    public NameNodeStub() {
        try {
            this.socketWithServer = new Socket(InetAddress.getLocalHost(), INameNodeProtocol.NAME_NODE_PORT);
        } catch (IOException e) {
            System.err.println("Can not connect to NameNode!");
            e.printStackTrace();
        }
    }


    /**
     * send request to NameNode and return the response
     * @param request sent to NameNode
     * @return response from NameNode
     */
    private NameNodeResponse sendRequest(NameNodeRequest request) {
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
    public SDFSFileChannelData openReadonly(String fileUri) throws FileNotFoundException {
        NameNodeRequest request = new NameNodeRequest(NameNodeRequest.Type.OPEN_READ_ONLY, fileUri, null, 0);
        NameNodeResponse response = sendRequest(request);
        assert response != null;
        if (response.getFileNotFoundException() != null) {
            throw response.getFileNotFoundException();
        } else {
            return response.getSDFSFileChannelData();
        }
    }

    @Override
    public SDFSFileChannelData openReadwrite(String fileUri) throws FileNotFoundException, OverlappingFileLockException {
        NameNodeRequest request = new NameNodeRequest(NameNodeRequest.Type.OPEN_READ_WRITE, fileUri, null, 0);
        NameNodeResponse response = sendRequest(request);
        assert response != null;
        if (response.getFileNotFoundException() != null) {
            throw response.getFileNotFoundException();
        } else if (response.getOverlappingFileLockException() != null) {
            throw response.getOverlappingFileLockException();
        } else {
            return response.getSDFSFileChannelData();
        }
    }

    @Override
    public SDFSFileChannelData create(String fileUri) throws SDFSFileAlreadyExistsException, FileNotFoundException {
        NameNodeRequest request = new NameNodeRequest(NameNodeRequest.Type.CREATE, fileUri, null, 0);
        NameNodeResponse response = sendRequest(request);
        assert response != null;
        if (response.getSDFSFileAlreadyExistsException() != null) {
            throw response.getSDFSFileAlreadyExistsException();
        } else if (response.getFileNotFoundException() != null) {
            throw response.getFileNotFoundException();
        } else {
            return response.getSDFSFileChannelData();
        }
    }

    @Override
    public void mkdir(String fileUri) throws SDFSFileAlreadyExistsException, FileNotFoundException {
        NameNodeRequest request = new NameNodeRequest(NameNodeRequest.Type.MK_DIR, fileUri, null, 0);
        NameNodeResponse response = sendRequest(request);
        assert response != null;
        if (response.getSDFSFileAlreadyExistsException() != null) {
            throw response.getSDFSFileAlreadyExistsException();
        } else if (response.getFileNotFoundException() != null) {
            throw response.getFileNotFoundException();
        }
    }

    @Override
    public void closeReadonlyFile(UUID fileAccessToken) throws IllegalAccessTokenException {
        NameNodeRequest request = new NameNodeRequest(NameNodeRequest.Type.CLOSE_READ_ONLY, null, fileAccessToken, 0);
        NameNodeResponse response = sendRequest(request);
        assert response != null;
        if (response.getIllegalAccessTokenException() != null) {
            throw response.getIllegalAccessTokenException();
        }
    }

    @Override
    public void closeReadwriteFile(UUID fileAccessToken, long newFileSize) throws IllegalAccessTokenException, IllegalArgumentException {
        NameNodeRequest request = new NameNodeRequest(NameNodeRequest.Type.CLOSE_READ_WRITE, null, fileAccessToken, newFileSize);
        NameNodeResponse response = sendRequest(request);
        assert response != null;
        if (response.getIllegalAccessTokenException() != null) {
            throw response.getIllegalAccessTokenException();
        } else if (response.getIllegalArgumentException() != null) {
            throw response.getIllegalArgumentException();
        }
    }

    @Override
    public List<LocatedBlock> addBlocks(UUID fileAccessToken, int blockAmount) throws IllegalAccessTokenException {
        NameNodeRequest request = new NameNodeRequest(NameNodeRequest.Type.ADD_BLOCKS, null, fileAccessToken, blockAmount);
        NameNodeResponse response = sendRequest(request);
        assert response != null;
        if (response.getIllegalAccessTokenException() != null) {
            throw response.getIllegalAccessTokenException();
        } else {
            return response.getBlockList();
        }
    }

    @Override
    public void removeLastBlocks(UUID fileAccessToken, int blockAmount) throws IllegalAccessTokenException, IndexOutOfBoundsException {
        NameNodeRequest request = new NameNodeRequest(NameNodeRequest.Type.REMOVE_LAST_BLOCKS, null, fileAccessToken, blockAmount);
        NameNodeResponse response = sendRequest(request);
        assert response != null;
        if (response.getIllegalAccessTokenException() != null) {
            throw response.getIllegalAccessTokenException();
        } else if (response.getIndexOutOfBoundsException() != null) {
            throw response.getIndexOutOfBoundsException();
        }
    }

    @Override
    public LocatedBlock newCopyOnWriteBlock(UUID fileAccessToken, int fileBlockNumber) throws IllegalAccessTokenException, IndexOutOfBoundsException {
        NameNodeRequest request = new NameNodeRequest(NameNodeRequest.Type.NEW_COW_BLOCK, null, fileAccessToken, fileBlockNumber);
        NameNodeResponse response = sendRequest(request);
        assert response != null;
        if (response.getIllegalAccessTokenException() != null) {
            throw response.getIllegalAccessTokenException();
        } else if (response.getIndexOutOfBoundsException() != null) {
            throw response.getIndexOutOfBoundsException();
        } else {
            return response.getBlockList().get(0); // put the Located Block in the first index of the List
        }
    }
}
