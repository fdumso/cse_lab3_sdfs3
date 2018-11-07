package response;

import java.io.FileNotFoundException;
import java.io.Serializable;

public class DataNodeResponse implements Serializable {
    private Type type;
    private byte[] data;

    private IndexOutOfBoundsException indexOutOfBoundsException;
    private FileNotFoundException fileNotFoundException;
    private IllegalStateException illegalStateException;


    public enum Type {
        READ, WRITE
    }

    public DataNodeResponse(Type type) {
        this.type = type;
    }

    public IndexOutOfBoundsException getIndexOutOfBoundsException() {
        return indexOutOfBoundsException;
    }

    public FileNotFoundException getFileNotFoundException() {
        return fileNotFoundException;
    }

    public IllegalStateException getIllegalStateException() {
        return illegalStateException;
    }

    public Type getType() {
        return type;
    }

    public byte[] getData() {
        return data;
    }


    public void setData(byte[] data) {
        this.data = data;
    }

    public void setIndexOutOfBoundsException(IndexOutOfBoundsException indexOutOfBoundsException) {
        this.indexOutOfBoundsException = indexOutOfBoundsException;
    }

    public void setFileNotFoundException(FileNotFoundException fileNotFoundException) {
        this.fileNotFoundException = fileNotFoundException;
    }

    public void setIllegalStateException(IllegalStateException illegalStateException) {
        this.illegalStateException = illegalStateException;
    }
}
