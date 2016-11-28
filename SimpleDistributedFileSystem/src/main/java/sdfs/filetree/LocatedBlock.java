package sdfs.filetree;

import java.io.Serializable;
import java.net.InetAddress;

public class LocatedBlock implements Serializable {
    private final InetAddress address;
    private final int port;
    private final int id;

    public LocatedBlock(InetAddress address, int port, int id) {
        if (address == null) {
            throw new NullPointerException();
        }
        this.address = address;
        this.id = id;
        this.port = port;
    }

    public InetAddress getAddress() {
        return address;
    }

    public int getId() {
        return id;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LocatedBlock that = (LocatedBlock) o;

        return id == that.id && address.equals(that.address);
    }

    @Override
    public int hashCode() {
        int result = address.hashCode();
        result = 31 * result + id;
        return result;
    }

    LocatedBlock copy() {
        return new LocatedBlock(this.address, this.port, this.id);
    }
}
