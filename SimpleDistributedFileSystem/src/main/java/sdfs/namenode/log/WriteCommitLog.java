package sdfs.namenode.log;

import java.util.UUID;

public class WriteCommitLog implements Log {
    private UUID token;
    private long newFileSize;

    public WriteCommitLog(UUID token, long newFileSize) {
        this.token = token;
        this.newFileSize = newFileSize;
    }

    public UUID getToken() {
        return token;
    }

    public void setToken(UUID token) {
        this.token = token;
    }

    public long getNewFileSize() {
        return newFileSize;
    }

    public void setNewFileSize(long newFileSize) {
        this.newFileSize = newFileSize;
    }
}
