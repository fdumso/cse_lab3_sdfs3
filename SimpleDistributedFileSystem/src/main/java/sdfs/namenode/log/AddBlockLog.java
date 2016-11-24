package sdfs.namenode.log;

import sdfs.filetree.LocatedBlock;

import java.util.UUID;

public class AddBlockLog implements Log {
    private UUID token;
    private LocatedBlock locatedBlock;

    public AddBlockLog(UUID token, LocatedBlock locatedBlock) {
        this.token = token;
        this.locatedBlock = locatedBlock;
    }

    public UUID getToken() {
        return token;
    }

    public void setToken(UUID token) {
        this.token = token;
    }

    public LocatedBlock getLocatedBlock() {
        return locatedBlock;
    }

    public void setLocatedBlock(LocatedBlock locatedBlock) {
        this.locatedBlock = locatedBlock;
    }
}
