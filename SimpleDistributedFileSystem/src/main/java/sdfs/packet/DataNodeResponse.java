package sdfs.packet;

import sdfs.exception.IllegalAccessTokenException;

import java.io.Serializable;

public class DataNodeResponse implements Serializable {
    private Type type;
    private byte[] data;

    private IllegalArgumentException illegalArgumentException;
    private IllegalAccessTokenException illegalAccessTokenException;


    public enum Type {
        READ, WRITE
    }

    public DataNodeResponse(Type type) {
        this.type = type;
    }

    public Type getType() {
        return type;
    }

    public byte[] getData() {
        return data;
    }

    public IllegalArgumentException getIllegalArgumentException() {
        return illegalArgumentException;
    }

    public IllegalAccessTokenException getIllegalAccessTokenException() {
        return illegalAccessTokenException;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public void setIllegalArgumentException(IllegalArgumentException illegalArgumentException) {
        this.illegalArgumentException = illegalArgumentException;
    }

    public void setIllegalAccessTokenException(IllegalAccessTokenException illegalAccessTokenException) {
        this.illegalAccessTokenException = illegalAccessTokenException;
    }
}
