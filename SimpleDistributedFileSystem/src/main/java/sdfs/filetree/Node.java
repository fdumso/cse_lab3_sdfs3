package sdfs.filetree;

import java.io.Serializable;

public abstract class Node implements Serializable {
    private Type type;

    public enum Type {
        FILE, DIR
        }

    public Node(Type type) {
        this.type = type;
    }

    public Type getType() {
        return type;
    }

}