package sdfs.namenode.log;

import java.io.Serializable;
import java.util.UUID;

public class CopyOnWriteBlockLog extends Log implements Serializable {
    private UUID token;
    private int fileBlockNumber;
    private int newBlockNumber;

    public CopyOnWriteBlockLog(int logID, UUID token, int fileBlockNumber, int newBlockNumber) {
        super(logID, Type.COPY_ON_WRITE_BLOCK);
        this.token = token;
        this.fileBlockNumber = fileBlockNumber;
        this.newBlockNumber = newBlockNumber;
    }

    public UUID getToken() {
        return token;
    }

    public int getFileBlockNumber() {
        return fileBlockNumber;
    }

    public int getNewBlockNumber() {
        return newBlockNumber;
    }
}
