package request;

import java.io.Serializable;
import java.util.UUID;

public class DataNodeRequest implements Serializable {
    private Type type;
    private UUID uuid;
    private int blockNumber, offset, size;
    private byte[] data;

    public DataNodeRequest(Type type, UUID uuid, int blockNumber, int offset, int size, byte[] data) {
        this.type = type;
        this.uuid = uuid;
        this.blockNumber = blockNumber;
        this.offset = offset;
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

    public int getOffset() {
        return offset;
    }

    public int getSize() {
        return size;
    }

    public byte[] getData() {
        return data;
    }
}
