package sdfs.namenode.log;

import java.util.UUID;

public class WriteAbortLog implements Log {
    private UUID token;

    public WriteAbortLog(UUID token) {
        this.token = token;
    }

    public UUID getToken() {
        return token;
    }

    public void setToken(UUID token) {
        this.token = token;
    }
}
