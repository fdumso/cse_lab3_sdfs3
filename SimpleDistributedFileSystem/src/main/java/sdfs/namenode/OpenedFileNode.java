package sdfs.namenode;

import sdfs.entity.FileInfo;
import sdfs.filetree.FileNode;

public class OpenedFileNode {
    private FileNode fileNode;
    private FileInfo fileInfo;

    public OpenedFileNode(FileNode fileNode, FileInfo fileInfo) {
        this.fileNode = fileNode;
        this.fileInfo = fileInfo;
    }

    /**
     * @param o that writing file
     * @return true if two writing files have the same original file
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OpenedFileNode that = (OpenedFileNode) o;
        return fileNode == that.fileNode;
    }

    public FileNode getFileNode() {
        return fileNode;
    }

    public FileInfo getFileInfo() {
        return fileInfo;
    }
}
