package sdfs.namenode.log;

import sdfs.filetree.LocatedBlock;

import java.util.UUID;

public class CopyOnWriteBlockLog implements Log {
    private UUID token;
    private int fileBlockNumber;
    private LocatedBlock locatedBlock;

    public CopyOnWriteBlockLog(UUID token, int fileBlockNumber, LocatedBlock locatedBlock) {
        this.token = token;
        this.fileBlockNumber = fileBlockNumber;
        this.locatedBlock = locatedBlock;
    }

    public UUID getToken() {
        return token;
    }

    public void setToken(UUID token) {
        this.token = token;
    }

    public int getFileBlockNumber() {
        return fileBlockNumber;
    }

    public void setFileBlockNumber(int fileBlockNumber) {
        this.fileBlockNumber = fileBlockNumber;
    }

    public LocatedBlock getLocatedBlock() {
        return locatedBlock;
    }

    public void setLocatedBlock(LocatedBlock locatedBlock) {
        this.locatedBlock = locatedBlock;
    }
}
