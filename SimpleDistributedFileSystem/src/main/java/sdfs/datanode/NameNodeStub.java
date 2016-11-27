package sdfs.datanode;

import sdfs.namenode.AccessTokenPermission;
import sdfs.packet.NameNodeRequest;
import sdfs.packet.NameNodeResponse;
import sdfs.protocol.INameNodeDataNodeProtocol;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.UUID;

public class NameNodeStub implements INameNodeDataNodeProtocol {
    private Socket socketWithNameNode;

    NameNodeStub(InetAddress address, int port) {
        try {
            this.socketWithNameNode = new Socket(address, port);
        } catch (IOException e) {
            System.err.println("Can not connect to NameNode!");
            e.printStackTrace();
        }
    }

    @Override
    public AccessTokenPermission getAccessTokenPermission(UUID token, InetAddress address) {
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
}
