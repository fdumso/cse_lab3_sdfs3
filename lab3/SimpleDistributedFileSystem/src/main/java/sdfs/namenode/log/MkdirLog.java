package sdfs.namenode.log;

import java.io.Serializable;

public class MkdirLog extends Log implements Serializable {
    private String fileUri;

    public MkdirLog(int logID, String fileUri) {
        super(logID, Type.MK_DIR);
        this.fileUri = fileUri;
    }

    public String getFileUri() {
        return fileUri;
    }
}
