package sdfs.namenode.log;

import java.io.Serializable;

public class CommitLog extends Log implements Serializable {

    public CommitLog(int id) {
        super(id, Type.COMMIT);
    }
}
