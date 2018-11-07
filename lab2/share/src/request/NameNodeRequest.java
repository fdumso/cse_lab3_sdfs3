package request;

import java.io.Serializable;
import java.util.UUID;

public class NameNodeRequest implements Serializable {
    private String string;
    private UUID uuid;
    private int integer;
    private Type type;

    public NameNodeRequest(Type type, String string, UUID uuid, int integer) {
        this.type = type;
        this.string = string;
        this.uuid = uuid;
        this.integer = integer;
    }

    public enum Type {
        OPEN_READ_ONLY, OPEN_READ_WRITE, CREATE, MK_DIR, CLOSE_READ_ONLY, CLOSE_READ_WRITE, ADD_BLOCKS, REMOVE_LAST_BLOCKS
    }

    public String getString() {
        return string;
    }

    public UUID getUuid() {
        return uuid;
    }

    public int getInteger() {
        return integer;
    }

    public Type getType() {
        return type;
    }
}
