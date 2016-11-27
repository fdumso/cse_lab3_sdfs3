package sdfs.filetree;

import java.io.Serializable;
import java.net.InetAddress;

public class LocatedBlock implements Serializable {
    private static final long serialVersionUID = -6509598325324530684L;
    private final InetAddress address;
    private final int port;
    private final int id;

    public LocatedBlock(InetAddress address, int port, int id) {
        if (address == null) {
            throw new NullPointerException();
        }
        this.address = address;
        this.port = port;
        this.id = id;
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

    LocatedBlock deepCopy() {
        return new LocatedBlock(this.address, this.port, this.id);
    }
}
