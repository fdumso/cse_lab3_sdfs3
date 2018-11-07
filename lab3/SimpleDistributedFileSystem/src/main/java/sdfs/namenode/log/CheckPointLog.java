package sdfs.namenode.log;

import java.io.Serializable;

public class CheckPointLog extends Log implements Serializable {
    public CheckPointLog(int id) {
        super(id, Type.CHECK_POINT);
    }
}
