package sdfs.namenode.log;

import java.io.Serializable;
import java.util.UUID;

public class CloseReadLog extends Log implements Serializable {

    private UUID token;

    public CloseReadLog(int logID, UUID token) {
        super(logID, Type.CLOSE_READ);
        this.token = token;
    }

    public UUID getToken() {
        return token;
    }
}
