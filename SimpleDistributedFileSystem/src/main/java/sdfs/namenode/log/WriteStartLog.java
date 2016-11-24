package sdfs.namenode.log;

import java.util.UUID;

public class WriteStartLog implements Log {
    private String fileUri;
    private UUID token;

    public WriteStartLog(String fileUri, UUID token) {
        this.fileUri = fileUri;
        this.token = token;
    }

    public String getFileUri() {
        return fileUri;
    }

    public void setFileUri(String fileUri) {
        this.fileUri = fileUri;
    }

    public UUID getToken() {
        return token;
    }

    public void setToken(UUID token) {
        this.token = token;
    }
}
