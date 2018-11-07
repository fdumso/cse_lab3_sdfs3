package sdfs.packet;

import sdfs.exception.IllegalAccessTokenException;

import java.io.Serializable;

public class DataNodeResponse implements Serializable {
    private byte[] data;

    private IllegalArgumentException illegalArgumentException;
    private IllegalAccessTokenException illegalAccessTokenException;

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
