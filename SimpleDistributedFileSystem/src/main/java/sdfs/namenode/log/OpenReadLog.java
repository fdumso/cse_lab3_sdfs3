package sdfs.namenode.log;

import java.io.Serializable;
import java.util.UUID;

public class OpenReadLog extends Log implements Serializable {
    private String fileUri;
    private UUID token;

    public OpenReadLog(int id, String fileUri, UUID token) {
        super(id, Type.OPEN_READ);
        this.fileUri = fileUri;
        this.token = token;
    }

    public String getFileUri() {
        return fileUri;
    }

    public UUID getToken() {
        return token;
    }
}
