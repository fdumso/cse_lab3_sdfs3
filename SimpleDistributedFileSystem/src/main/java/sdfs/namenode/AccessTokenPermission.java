package sdfs.namenode;

import java.io.Serializable;
import java.util.Set;

public class AccessTokenPermission implements Serializable {
    private static final long serialVersionUID = -6174811460052859447L;
    private boolean writable;
    private Set<Integer> allowedBlocks;

    public AccessTokenPermission(boolean writable, Set<Integer> allowBlocks) {
        this.writable = writable;
        this.allowedBlocks = allowBlocks;
    }

    public boolean isWritable() {
        return writable;
    }

    public Set<Integer> getAllowedBlocks() {
        return allowedBlocks;
    }
}
