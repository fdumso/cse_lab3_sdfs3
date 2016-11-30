package sdfs.namenode.log;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

public class AddBlocksLog extends Log implements Serializable {
    private UUID token;
    private List<Integer> newBlockNumberList;

    public AddBlocksLog(int logID, UUID token, List<Integer> newBlockNumberList) {
        super(logID, Type.ADD_BLOCKS);
        this.token = token;
        this.newBlockNumberList = newBlockNumberList;
    }

    public UUID getToken() {
        return token;
    }

    public List<Integer> getNewBlockNumberList() {
        return newBlockNumberList;
    }
}
