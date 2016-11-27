package sdfs.namenode;

import sdfs.filetree.FileNode;

import java.io.Serializable;
import java.util.UUID;

public class SDFSFileChannelData implements Serializable {
    private static final long serialVersionUID = 5725498307666004432L;

    private UUID token;
    private FileNode fileNode;
    private boolean writable;

    SDFSFileChannelData(FileNode fileNode, boolean writable, UUID token) {
        this.token = token;
        this.fileNode = fileNode;
        this.writable = writable;
    }

    public UUID getToken() {
        return token;
    }

    public boolean isWritable() {
        return writable;
    }

    public FileNode getFileNode() {
        return fileNode;
    }
}
