package sdfs.namenode;

import sdfs.filetree.FileNode;

import java.io.Serializable;
import java.util.UUID;

public class SDFSFileChannelData implements Serializable {
    private static final long serialVersionUID = 5725498307666004432L;

    private UUID token;
    private FileNode fileNode;
    private long fileSize; //Size of this file
    private int blockAmount; //Total block amount of this file
    private AccessTokenPermission permission;

    public SDFSFileChannelData(FileNode fileNode, long fileSize, int blockAmount, AccessTokenPermission permission, UUID token) {
        this.token = token;
        this.fileNode = fileNode;
        this.fileSize = fileSize;
        this.blockAmount = blockAmount;
        this.permission = permission;
    }

    public UUID getToken() {
        return token;
    }

    public AccessTokenPermission getPermission() {
        return permission;
    }

    public FileNode getFileNode() {
        return fileNode;
    }

    public long getFileSize() {
        return fileSize;
    }

    public int getBlockAmount() {
        return blockAmount;
    }
}
