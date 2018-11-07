package sdfs.namenode;

import sdfs.entity.DirNode;

public class DirEntry extends Entry {
    private DirNode node;
    public DirEntry(String name, int id) {
        super(name, id, NodeType.DIR);
        this.node = new DirNode();
    }

    public void addEntry(Entry entry) {
        node.addEntry(entry);
    }

    public Entry findChild(String name) {
        return node.findChild(name);
    }

    public byte[] toBytes() {
        return node.toBytes();
    }

}
