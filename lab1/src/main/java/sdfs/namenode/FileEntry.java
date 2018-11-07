package sdfs.namenode;

import sdfs.entity.FileNode;

public class FileEntry extends Entry {
    private FileNode node;
    public FileEntry(String name, int id, FileNode node) {
        super(name, id, NodeType.FILE);
        this.node = node;
    }

    public FileNode getNode() {
        return node;
    }
}
