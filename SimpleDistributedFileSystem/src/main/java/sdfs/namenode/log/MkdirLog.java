package sdfs.namenode.log;

public class MkdirLog implements Log {
    private String fileUri;

    public MkdirLog(String fileUri) {
        this.fileUri = fileUri;
    }

    public String getFileUri() {
        return fileUri;
    }

    public void setFileUri(String fileUri) {
        this.fileUri = fileUri;
    }
}
