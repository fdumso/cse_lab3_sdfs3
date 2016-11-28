package sdfs.filetree;

import sdfs.namenode.DataBlockManager;

import java.io.Serializable;

public abstract class Node implements Serializable {
    private Type type;

    public enum Type {
        FILE, DIR
        }

    Node(Type type) {
        this.type = type;
    }

    public Type getType() {
        return type;
    }

    public abstract void recordExistence(DataBlockManager dataBlockManager);

}