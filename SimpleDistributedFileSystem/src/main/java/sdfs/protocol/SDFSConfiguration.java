package sdfs.protocol;

import java.net.InetAddress;

public class SDFSConfiguration {
    private InetAddress nameNodeAddress;
    private InetAddress dataNodeAddress;
    private int nameNodePort;
    private int dataNodePort;

    public SDFSConfiguration(InetAddress nameNodeAddress, int nameNodePort, InetAddress dataNodeAddress, int dataNodePort) {
        this.nameNodeAddress = nameNodeAddress;
        this.dataNodeAddress = dataNodeAddress;
        this.nameNodePort = nameNodePort;
        this.dataNodePort = dataNodePort;
    }

    public InetAddress getNameNodeAddress() {
        return nameNodeAddress;
    }

    public InetAddress getDataNodeAddress() {
        return dataNodeAddress;
    }

    public int getNameNodePort() {
        return nameNodePort;
    }

    public int getDataNodePort() {
        return dataNodePort;
    }
}
