package sdfs.namenode;

import java.io.Serializable;
import java.util.Set;

/**
 * Created by pengcheng on 2016/11/15.
 */
public class AccessTokenPermission implements Serializable {
    private static final long serialVersionUID = -6174811460052859447L;
    private boolean writeable;
    private Set<Integer> allowBlocks;

    public AccessTokenPermission(boolean writeable, Set<Integer> allowBlocks) {
        this.writeable = writeable;
        this.allowBlocks = allowBlocks;
    }
}