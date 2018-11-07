package sdfs.namenode;

public abstract class Entry {
    private String name;
    private int id;
    private NodeType nodeType;

    public Entry(String name, int id, NodeType nodeType) {
        this.name = name;
        this.id = id;
        this.nodeType = nodeType;
    }

    public String getName() {
        return name;
    }

    public int getId() {
        return id;
    }

    public NodeType getType() {
        return nodeType;
    }

    public enum NodeType {
        FILE, DIR
    }


}
