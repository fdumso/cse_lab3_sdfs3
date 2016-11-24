package sdfs.namenode.log;

import java.util.UUID;

public class RemoveBlocksLog implements Log {
    private UUID token;
    private int blockAmount;

    public RemoveBlocksLog(UUID token) {
        this.token = token;
    }

    public UUID getToken() {
        return token;
    }

    public void setToken(UUID token) {
        this.token = token;
    }

    public int getBlockAmount() {
        return blockAmount;
    }

    public void setBlockAmount(int blockAmount) {
        this.blockAmount = blockAmount;
    }
}
