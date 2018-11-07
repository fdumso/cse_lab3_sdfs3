package sdfs.datanode;

import sdfs.entity.AccessTokenPermission;
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
    private InetAddress address;
    private int port;

    NameNodeStub(InetAddress address, int port) {
        this.address = address;
        this.port = port;
    }

    @Override
    public AccessTokenPermission getAccessTokenPermission(UUID token, InetAddress dataNodeAddress) {
        NameNodeRequest request = new NameNodeRequest(NameNodeRequest.Type.GET_ACCESS_TOKEN_PERMISSION, null, token, 0);
        NameNodeResponse response = null;
        try {
            Socket socket = new Socket(this.address, port);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
            objectOutputStream.writeObject(request);
            objectOutputStream.flush();
            ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
            response = (NameNodeResponse) objectInputStream.readObject();
            socket.close();
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
