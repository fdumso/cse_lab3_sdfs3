package sdfs.namenode.log;

public class CreateFileLog implements Log {
    private String fileUri;

    public CreateFileLog(String fileUri) {
        this.fileUri = fileUri;
    }

    public String getFileUri() {
        return fileUri;
    }

    public void setFileUri(String fileUri) {
        this.fileUri = fileUri;
    }
}
