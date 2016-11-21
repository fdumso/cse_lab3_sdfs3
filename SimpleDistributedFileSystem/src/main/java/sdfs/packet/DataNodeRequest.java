package sdfs.packet;

import java.io.Serializable;
import java.util.UUID;

public class DataNodeRequest implements Serializable {
    private Type type;
    private UUID uuid;
    private int blockNumber, size;
    private long position;
    private byte[] data;

    public DataNodeRequest(Type type, UUID uuid, int blockNumber, long position, int size, byte[] data) {
        this.type = type;
        this.uuid = uuid;
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

    public UUID getUuid() {
        return uuid;
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
