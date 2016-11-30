package sdfs.namenode.log;

import java.io.Serializable;

public class StartLog extends Log  implements Serializable {

    public StartLog(int id) {
        super(id, Type.START);
    }

}
