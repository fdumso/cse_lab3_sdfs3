package sdfs.packet;

import java.io.Serializable;
import java.util.UUID;

public class DataNodeRequest implements Serializable {
    private Type type;
    private UUID token;
    private int blockNumber, size;
    private long position;
    private byte[] data;

    public DataNodeRequest(Type type, UUID token, int blockNumber, long position, int size, byte[] data) {
        this.type = type;
        this.token = token;
        this.blockNumber = blockNumber;
        this.position = position;
        this.size = size;
        this.data = data;
    }

    public enum Type {
        READ, WRITE
    }

    public Type getType() {
        return type;
    }

    public UUID getToken() {
        return token;
    }

    public int getBlockNumber() {
        return blockNumber;
    }

    public long getPosition() {
        return position;
    }

    public int getSize() {
        return size;
    }

    public byte[] getData() {
        return data;
    }
}
