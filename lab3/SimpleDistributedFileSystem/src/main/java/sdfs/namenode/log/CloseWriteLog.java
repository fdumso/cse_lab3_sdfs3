package sdfs.namenode.log;

import java.io.Serializable;
import java.util.UUID;

public class CloseWriteLog extends Log implements Serializable {
    private UUID token;
    private long newFileSize;

    public CloseWriteLog(int logID, UUID token, long newFileSize) {
        super(logID, Type.CLOSE_WRITE);
        this.token = token;
        this.newFileSize = newFileSize;
    }

    public UUID getToken() {
        return token;
    }

    public long getNewFileSize() {
        return newFileSize;
    }
}
