package sdfs.namenode.log;

import java.io.Serializable;

public class AbortLog extends Log implements Serializable {

    public AbortLog(int id) {
        super(id, Type.ABORT);
    }

}
