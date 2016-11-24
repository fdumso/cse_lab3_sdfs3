package sdfs.packet;

import java.io.Serializable;
import java.util.UUID;

public class NameNodeRequest implements Serializable {
    private String string;
    private UUID token;
    private long number;
    private Type type;

    public NameNodeRequest(Type type, String string, UUID token, long number) {
        this.type = type;
        this.string = string;
        this.token = token;
        this.number = number;
    }

    public enum Type {
        OPEN_READ_ONLY, OPEN_READ_WRITE, CREATE, MK_DIR, CLOSE_READ_ONLY, CLOSE_READ_WRITE,
        ADD_BLOCKS, REMOVE_LAST_BLOCKS, NEW_COW_BLOCK,
        GET_ACCESS_TOKEN_PERMISSION
    }

    public String getString() {
        return string;
    }

    public UUID getToken() {
        return token;
    }

    public long getNumber() {
        return number;
    }

    public Type getType() {
        return type;
    }
}
