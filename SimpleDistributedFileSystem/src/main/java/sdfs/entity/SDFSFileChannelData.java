package sdfs.entity;

import java.io.Serializable;
import java.util.UUID;

public class SDFSFileChannelData implements Serializable {
    private static final long serialVersionUID = 5725498307666004432L;

    private UUID token;
    private FileInfo fileInfo;
    private boolean writable;

    public SDFSFileChannelData(FileInfo fileInfo, boolean writable, UUID token) {
        this.token = token;
        this.fileInfo = fileInfo;
        this.writable = writable;
    }

    public UUID getToken() {
        return token;
    }

    public boolean isWritable() {
        return writable;
    }

    public FileInfo getFileInfo() {
        return fileInfo;
    }
}
