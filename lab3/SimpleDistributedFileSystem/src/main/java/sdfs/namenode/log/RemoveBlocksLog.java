package sdfs.namenode.log;

import java.io.Serializable;
import java.util.UUID;

public class RemoveBlocksLog extends Log implements Serializable {
    private UUID token;
    private int blockAmount;

    public RemoveBlocksLog(int logID, UUID token, int blockAmount) {
        super(logID, Type.REMOVE_BLOCKS);
        this.token = token;
        this.blockAmount = blockAmount;
    }

    public UUID getToken() {
        return token;
    }

    public int getBlockAmount() {
        return blockAmount;
    }

}
